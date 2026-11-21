"""Simula API, eventos S3/SQS e dependências AWS das Lambdas; não chama a conta AWS."""
import hashlib
import io
import importlib.util
import json
import os
import sys
import types
from pathlib import Path

import pytest


os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["BRONZE_BUCKET"] = "bronze-test"
os.environ["RESULTS_BUCKET"] = "results-test"
os.environ["QUARANTINE_BUCKET"] = "quarantine-test"
os.environ["READY_QUEUE_URL"] = "https://sqs.example/ready"
os.environ["LEDGER_TABLE"] = "ledger-test"
os.environ["STATE_MACHINE_ARN"] = "arn:aws:states:us-east-1:111111111111:stateMachine:test"
os.environ["MAX_UPLOAD_BYTES"] = "100"
sys.modules["boto3"] = types.SimpleNamespace(client=lambda _service: object())
botocore_exceptions = types.ModuleType("botocore.exceptions")


class FakeClientError(Exception):
    pass


botocore_exceptions.ClientError = FakeClientError
sys.modules["botocore"] = types.ModuleType("botocore")
sys.modules["botocore.exceptions"] = botocore_exceptions
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("api_ingestion", ROOT / "terraform/modules/ingestion/lambdas/api_ingestion.py")
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)
spec = importlib.util.spec_from_file_location("receipt", ROOT / "terraform/modules/ingestion/lambdas/receipt.py")
receipt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(receipt)
spec = importlib.util.spec_from_file_location("orchestrator", ROOT / "terraform/modules/ingestion/lambdas/orchestrator.py")
orchestrator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(orchestrator)


class S3Stub:
    def generate_presigned_url(self, operation, Params, ExpiresIn):
        self.operation, self.params, self.expires = operation, Params, ExpiresIn
        return "https://example.invalid/upload"


def test_api_returns_checksum_and_trace_metadata(monkeypatch):
    stub = S3Stub()
    monkeypatch.setattr(api, "s3", stub)
    digest = hashlib.sha256(b"pedido;P1\n").hexdigest()
    event = {"body": json.dumps({"file_name": "pedidos.csv", "content_type": "text/csv",
                                   "source_system": "erp", "schema_version": "1", "size_bytes": 10, "sha256": digest})}
    result = api.handler(event, None)
    body = json.loads(result["body"])
    assert result["statusCode"] == 201
    assert body["required_headers"]["x-amz-checksum-sha256"]
    assert stub.params["Metadata"]["sha256"] == digest
    assert stub.params["Metadata"]["expected-size"] == "10"
    assert stub.params["Key"].startswith("ingestion/erp/")


def test_api_rejects_invalid_hash(monkeypatch):
    monkeypatch.setattr(api, "s3", S3Stub())
    result = api.handler({"body": json.dumps({"file_name": "pedidos.csv", "content_type": "text/csv",
                                                "source_system": "erp", "schema_version": "1", "size_bytes": 10, "sha256": "bad"})}, None)
    assert result["statusCode"] == 400


def test_api_rejects_size_above_limit(monkeypatch):
    monkeypatch.setattr(api, "s3", S3Stub())
    digest = hashlib.sha256(b"x").hexdigest()
    result = api.handler({"body": json.dumps({"file_name": "pedidos.csv", "content_type": "text/csv",
                                                "source_system": "erp", "schema_version": "1", "size_bytes": 101, "sha256": digest})}, None)
    assert result["statusCode"] == 400
    assert json.loads(result["body"])["code"] == "VALIDATION_ERROR"


class FailingPresignS3:
    def generate_presigned_url(self, *_args, **_kwargs):
        raise RuntimeError("S3 indisponível")


def test_api_returns_503_when_presign_dependency_fails(monkeypatch):
    monkeypatch.setattr(api, "s3", FailingPresignS3())
    digest = hashlib.sha256(b"x").hexdigest()
    result = api.handler({"body": json.dumps({"file_name": "pedidos.csv", "content_type": "text/csv",
                                                "source_system": "erp", "schema_version": "1", "size_bytes": 1, "sha256": digest})}, None)
    assert result["statusCode"] == 503
    assert json.loads(result["body"])["code"] == "PRESIGN_UNAVAILABLE"


class ReceiptS3:
    def __init__(self, payload=b"abc", expected_size=None, metadata_override=None):
        self.payload = payload
        self.expected_size = expected_size if expected_size is not None else len(payload)
        self.metadata_override = metadata_override or {}
        self.copies = []

    def head_object(self, **_kwargs):
        metadata = {"ingestion-id": "11111111-1111-1111-1111-111111111111", "sha256": hashlib.sha256(self.payload).hexdigest(),
                    "source-system": "erp", "schema-version": "1", "expected-size": str(self.expected_size)}
        metadata.update(self.metadata_override)
        return {"Metadata": metadata,
                "ContentLength": len(self.payload), "ContentType": "text/csv"}

    def get_object(self, **_kwargs):
        return {"Body": io.BytesIO(self.payload)}

    def copy_object(self, **kwargs):
        self.copies.append(kwargs)

    def put_object(self, **_kwargs):
        return {}


class DdbStub:
    def __init__(self):
        self.updates = []

    def update_item(self, **kwargs):
        self.updates.append(kwargs)
        return {}

    def put_item(self, **_kwargs):
        return {}


class SqsStub:
    def __init__(self, fail=False):
        self.fail, self.messages = fail, []

    def send_message(self, **kwargs):
        if self.fail:
            raise RuntimeError("SQS indisponível")
        self.messages.append(kwargs)
        return {}


def receipt_event():
    s3_event = {"s3": {"bucket": {"name": "bronze-test"}, "object": {"key": "ingestion/erp/pedidos.csv", "versionId": "v1"}}}
    return {"Records": [{"messageId": "receipt-1", "body": json.dumps({"Records": [s3_event]})}]}


@pytest.mark.parametrize(("file_name", "expected_size", "metadata_override", "reason"), [
    ("metadados_ausentes.csv", None, {"expected-size": None}, "metadados obrigatórios ausentes"),
    ("tamanho_divergente.csv", 999, {}, "tamanho do objeto divergente"),
    ("hash_divergente.csv", None, {"sha256": "0" * 64}, "sha256 divergente"),
])
def test_receipt_quarantines_permanent_error_files_and_acknowledges_message(
    monkeypatch, file_name, expected_size, metadata_override, reason
):
    payload = (ROOT / "dados" / "erros" / file_name).read_bytes()
    s3, ddb = ReceiptS3(payload=payload, expected_size=expected_size, metadata_override=metadata_override), DdbStub()
    monkeypatch.setattr(receipt, "s3", s3)
    monkeypatch.setattr(receipt, "ddb", ddb)
    monkeypatch.setattr(receipt, "sqs", SqsStub())
    result = receipt.handler(receipt_event(), None)
    assert result == {"batchItemFailures": []}
    assert len(s3.copies) == 1
    assert ddb.updates[0]["ExpressionAttributeValues"][":status"]["S"] == "REJECTED"
    assert ddb.updates[0]["ExpressionAttributeValues"][":reason"]["S"] == reason


def test_receipt_retries_operational_failure(monkeypatch):
    monkeypatch.setattr(receipt, "s3", ReceiptS3())
    monkeypatch.setattr(receipt, "ddb", DdbStub())
    monkeypatch.setattr(receipt, "sqs", SqsStub(fail=True))
    assert receipt.handler(receipt_event(), None) == {"batchItemFailures": [{"itemIdentifier": "receipt-1"}]}


class SfnUnavailable:
    def start_execution(self, **_kwargs):
        raise RuntimeError("Step Functions indisponível")


def test_orchestrator_marks_retry_and_returns_message_to_sqs(monkeypatch):
    ddb = DdbStub()
    monkeypatch.setattr(orchestrator, "sfn", SfnUnavailable())
    monkeypatch.setattr(orchestrator, "ddb", ddb)
    event = {"Records": [{"messageId": "ready-1", "body": json.dumps({
        "ingestion_id": "11111111-1111-1111-1111-111111111111", "bronze_manifest": "manifestos/bronze/x.json", "object_version": "bronze-test/key/v1"})}]}
    assert orchestrator.handler(event, None) == {"batchItemFailures": [{"itemIdentifier": "ready-1"}]}
    values = ddb.updates[0]["ExpressionAttributeValues"]
    assert values[":status"]["S"] == "ORCHESTRATION_RETRY"
    assert "indisponível" in values[":reason"]["S"]
