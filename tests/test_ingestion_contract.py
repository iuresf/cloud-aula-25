"""Contratos e integrações simuladas; nenhum acesso à AWS."""
import hashlib
import importlib.util
import io
import json
import os
import sys
import types
from pathlib import Path

import pytest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from lakehouse.contracts import validate_manifest
os.environ.update(AWS_EC2_METADATA_DISABLED="true", AWS_DEFAULT_REGION="us-east-1",
                  BRONZE_BUCKET="bronze", RESULTS_BUCKET="results", QUARANTINE_BUCKET="quarantine",
                  BATCH_TABLE="batches", LEDGER_TABLE="ledger", READY_QUEUE_URL="ready",
                  DATABASE="lab", MAX_UPLOAD_BYTES="1000000", STATE_MACHINE_ARN="arn:aws:states:us-east-1:123:stateMachine:test", OPERATIONS_TABLE="ops")


class ConditionalError(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class FakeKey:
    def __init__(self, name): self.name = name
    def eq(self, value): return value


class Table:
    def __init__(self): self.items = {}
    def put_item(self, Item, ConditionExpression=None):
        key = (Item["batch_id"], Item["record_id"])
        if ConditionExpression and key in self.items: raise ConditionalError()
        self.items[key] = Item
    def get_item(self, Key, **_kwargs):
        item = self.items.get((Key["batch_id"], Key["record_id"]))
        return {"Item": item} if item else {}
    def query(self, KeyConditionExpression, **_kwargs):
        return {"Items": [item for (batch, _), item in self.items.items() if batch == KeyConditionExpression]}
    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues, **_kwargs):
        item = self.items.setdefault((Key["batch_id"], Key["record_id"]), {**Key})
        if "SET #s=:ready" in UpdateExpression: item.update(status="READY", bronze_manifest=ExpressionAttributeValues[":manifest"])
        elif "SET #s=:received" in UpdateExpression: item["status"] = "RECEIVED"
        elif "SET #s=:rejected" in UpdateExpression: item["status"] = "REJECTED"
        elif "SET #s=:processing" in UpdateExpression: item.update(status="PROCESSING_SILVER", run_id=ExpressionAttributeValues[":run"])


class Resource:
    def __init__(self, table):
        self.table = table
        self.meta = types.SimpleNamespace(client=types.SimpleNamespace(exceptions=types.SimpleNamespace(ConditionalCheckFailedException=ConditionalError)))
    def Table(self, _name): return self.table


class S3:
    def __init__(self): self.objects = {}; self.copies = []; self.presigns = []
    def generate_presigned_url(self, _operation, Params, **_kwargs):
        self.presigns.append(Params)
        return "https://example.invalid/upload"
    def put_object(self, Bucket, Key, Body, **_kwargs): self.objects[(Bucket, Key)] = Body; return {}
    def head_object(self, **_kwargs):
        body = b"abc"
        return {"Metadata": {"ingestion-id": "upload-1", "batch-id": "batch_1", "dataset-type": "pedidos",
                             "source-system": "erp", "schema-version": "1", "expected-size": "3",
                             "sha256": hashlib.sha256(body).hexdigest()}, "ContentLength": 3, "ContentType": "text/csv"}
    def get_object(self, Bucket, Key, **_kwargs): return {"Body": io.BytesIO(self.objects.get((Bucket, Key), b"abc"))}
    def copy_object(self, **kwargs): self.copies.append(kwargs)


class DDB:
    def __init__(self): self.rows = {}; self.updates = []; self.lock = None
    def put_item(self, TableName, Item, **_kwargs):
        if TableName == "ops":
            if self.lock: raise ConditionalError()
            self.lock = Item
            return
        key = Item["object_version"]["S"]
        if key in self.rows: raise ConditionalError()
        self.rows[key] = Item
    def update_item(self, **kwargs): self.updates.append(kwargs)
    def get_item(self, **_kwargs): return {"Item": self.lock} if self.lock else {}
    def delete_item(self, **_kwargs): self.lock = None


class SQS:
    def __init__(self): self.sent = []
    def send_message(self, **kwargs): self.sent.append(kwargs)


class SFN:
    def __init__(self): self.calls = []
    def start_execution(self, **kwargs):
        self.calls.append(kwargs)
        return {"executionArn": "arn:aws:states:us-east-1:123:execution:test:batch"}


s3, table, ledger, sqs, sfn = S3(), Table(), DDB(), SQS(), SFN()
resource = Resource(table)
boto = types.ModuleType("boto3")
boto.client = lambda name: {"s3": s3, "dynamodb": ledger, "sqs": sqs, "stepfunctions": sfn,
                            "glue": types.SimpleNamespace(get_table=lambda **_kw: {})}[name]
boto.resource = lambda _name: resource
sys.modules["boto3"] = boto
sys.modules["boto3.dynamodb"] = types.ModuleType("boto3.dynamodb")
conditions = types.ModuleType("boto3.dynamodb.conditions")
conditions.Key = FakeKey
sys.modules["boto3.dynamodb.conditions"] = conditions
botocore = types.ModuleType("botocore")
errors = types.ModuleType("botocore.exceptions")
errors.ClientError = ConditionalError
sys.modules["botocore"] = botocore
sys.modules["botocore.exceptions"] = errors


def load(name):
    path = ROOT / "terraform/modules/ingestion/lambdas" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


api, receipt, orchestrator = (load(name) for name in ("api_ingestion", "receipt", "orchestrator"))


@pytest.fixture(autouse=True)
def reset():
    table.items.clear(); ledger.rows.clear(); ledger.updates.clear(); ledger.lock = None; sqs.sent.clear(); sfn.calls.clear(); s3.objects.clear(); s3.copies.clear(); s3.presigns.clear()


def upload_payload(dataset="pedidos"):
    return {"batch_id": "batch_1", "operation": "initial", "dataset_type": dataset,
            "file_name": f"{dataset}.csv", "sha256": hashlib.sha256(b"abc").hexdigest(),
            "source_system": "erp", "schema_version": "1", "content_type": "text/csv", "size_bytes": 3}


def test_upload_records_pending_session_and_metadata():
    result = api.handler({"body": json.dumps(upload_payload())}, None)
    assert result["statusCode"] == 201
    assert s3.presigns[0]["Metadata"]["dataset-type"] == "pedidos"
    assert s3.presigns[0]["Key"].startswith("ingestion/pedidos/")
    assert any(key[1].startswith("UPLOAD#") and value["status"] == "PENDING" for key, value in table.items.items())


def test_invalid_sha_rejected():
    payload = upload_payload(); payload["sha256"] = "wrong"
    assert api.handler({"body": json.dumps(payload)}, None)["statusCode"] == 400


def test_finalize_json_gets_its_own_presigned_control_upload():
    body = b'{"type":"batch_finalize"}'
    payload = {"batch_id": "batch_1", "operation": "initial", "dataset_type": "batch_finalize",
               "file_name": "finalize.json", "sha256": hashlib.sha256(body).hexdigest(),
               "source_system": "erp", "schema_version": "1", "content_type": "application/json", "size_bytes": len(body)}
    result = api.handler({"body": json.dumps(payload)}, None)
    assert result["statusCode"] == 201
    assert s3.presigns[0]["Key"].startswith("ingestion/_control/")


def test_finalize_is_not_an_api_action():
    result = api.handler({"body": json.dumps({"action": "finalize", "batch_id": "batch_1"})}, None)
    assert result["statusCode"] == 400 and not sqs.sent


def receipt_event():
    return {"Records": [{"messageId": "m1", "body": json.dumps({"Records": [{"s3": {
        "bucket": {"name": "bronze"}, "object": {"key": "ingestion/pedidos/erp/f.csv", "versionId": "v1"}}}]})}]}


def test_receipt_stores_exact_version_and_duplicate_is_idempotent():
    table.put_item({"batch_id": "batch_1", "record_id": "META", "operation": "initial", "status": "WAITING_FOR_BATCH"})
    table.put_item({"batch_id": "batch_1", "record_id": "UPLOAD#upload-1", "s3_key": "ingestion/pedidos/erp/f.csv",
                    "sha256": hashlib.sha256(b"abc").hexdigest(), "status": "PENDING"})
    assert receipt.handler(receipt_event(), None) == {"batchItemFailures": []}
    assert table.items[("batch_1", "UPLOAD#upload-1")]["status"] == "RECEIVED"
    assert table.items[("batch_1", "FILE#bronze/ingestion/pedidos/erp/f.csv/v1")]["version_id"] == "v1"
    assert receipt.handler(receipt_event(), None) == {"batchItemFailures": []}


def test_receipt_rejects_missing_metadata(monkeypatch):
    original = s3.head_object
    monkeypatch.setattr(s3, "head_object", lambda **kw: {**original(**kw), "Metadata": {"ingestion-id": "upload-1"}})
    assert receipt.handler(receipt_event(), None) == {"batchItemFailures": []}
    assert len(s3.copies) == 1
    assert ledger.updates[0]["ExpressionAttributeValues"][":rejected"]["S"] == "REJECTED"


def test_receipt_hash_mismatch_quarantined(monkeypatch):
    original = s3.head_object
    monkeypatch.setattr(s3, "head_object", lambda **kw: {**original(**kw), "Metadata": {**original(**kw)["Metadata"], "sha256": "0"*64}})
    assert receipt.handler(receipt_event(), None) == {"batchItemFailures": []}
    assert len(s3.copies) == 1


def test_orchestrator_requires_ready_batch():
    table.put_item({"batch_id": "batch_1", "record_id": "META", "status": "WAITING_FOR_BATCH", "bronze_manifest": "m.json"})
    event = {"Records": [{"messageId": "m1", "body": json.dumps({"batch_id": "batch_1", "bronze_manifest": "m.json"})}]}
    assert orchestrator.handler(event, None) == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert not sfn.calls


def test_orchestrator_starts_once_for_duplicate_delivery():
    table.put_item({"batch_id": "batch_1", "record_id": "META", "status": "READY", "bronze_manifest": "m.json"})
    event = {"Records": [{"messageId": "m1", "body": json.dumps({"batch_id": "batch_1", "bronze_manifest": "m.json"})}]}
    assert orchestrator.handler(event, None) == {"batchItemFailures": []}
    assert orchestrator.handler(event, None) == {"batchItemFailures": []}
    assert sfn.calls[0]["name"] == sfn.calls[1]["name"]


def test_manifest_rejects_missing_initial_source():
    with pytest.raises(ValueError, match="incompleto"):
        validate_manifest({"layer": "bronze", "batch_id": "batch_1", "operation": "initial",
                           "files": [{"s3_key": "pedidos/x.csv", "version_id": "v1", "sha256": "a"*64}]})


def test_receipt_retries_transient_s3_failure(monkeypatch):
    monkeypatch.setattr(s3, "head_object", lambda **_kw: (_ for _ in ()).throw(RuntimeError("S3 temporariamente indisponível")))
    assert receipt.handler(receipt_event(), None) == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert not s3.copies


def test_orchestrator_retries_start_failure_and_releases_lock(monkeypatch):
    table.put_item({"batch_id": "batch_1", "record_id": "META", "status": "READY", "bronze_manifest": "m.json"})
    event = {"Records": [{"messageId": "m1", "body": json.dumps({"batch_id": "batch_1", "bronze_manifest": "m.json"})}]}
    monkeypatch.setattr(sfn, "start_execution", lambda **_kw: (_ for _ in ()).throw(RuntimeError("Step Functions temporariamente indisponível")))
    assert orchestrator.handler(event, None) == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert ledger.lock is None
