"""Cria uploads versionados; o arquivo finalize.json fecha o lote."""
import base64
import json
import os
import re
import uuid
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
DATASETS = {"pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"}
FINALIZE = "batch_finalize"
ALLOWED = {"text/csv", "application/json", "application/x-ndjson"}


def response(status, body):
    return {"statusCode": status, "headers": {"content-type": "application/json"}, "body": json.dumps(body)}


def handler(event, _context):
    try:
        raw = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw).decode()
        payload = json.loads(raw)
        return create_upload(payload)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return response(400, {"code": "VALIDATION_ERROR", "message": str(exc)})


def create_upload(payload):
    name = payload["file_name"]
    sha256 = payload["sha256"].lower()
    source = payload["source_system"].lower()
    schema_version = str(payload["schema_version"])
    content_type = payload["content_type"]
    size_bytes = payload["size_bytes"]
    batch_id = payload["batch_id"]
    dataset = payload["dataset_type"]
    operation = payload["operation"]
    if dataset not in DATASETS | {FINALIZE} or operation not in ("initial", "incremental"):
        raise ValueError("dataset_type ou operation inválido")
    if not re.fullmatch(r"[A-Za-z0-9_-]{2,64}", batch_id):
        raise ValueError("batch_id inválido")
    if content_type not in ALLOWED or not re.fullmatch(r"[a-f0-9]{64}", sha256):
        raise ValueError("content_type ou sha256 inválido")
    if not re.fullmatch(r"[a-z0-9_-]{2,40}", source) or not re.fullmatch(r"[A-Za-z0-9._-]{1,180}", name):
        raise ValueError("source_system ou file_name inválido")
    if schema_version != "1":
        raise ValueError("schema_version não suportado")
    expected_types = {"text/csv"} if dataset in {"pedidos", "itens", "clientes", "produtos"} else {"application/json", "application/x-ndjson"}
    if content_type not in expected_types:
        raise ValueError("content_type incompatível com dataset_type")
    if dataset == FINALIZE and name != "finalize.json":
        raise ValueError("o arquivo de fechamento deve se chamar finalize.json")
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or not 0 < size_bytes <= int(os.environ["MAX_UPLOAD_BYTES"]):
        raise ValueError("size_bytes inválido ou acima do limite")
    table = ddb.Table(os.environ["BATCH_TABLE"])
    try:
        table.put_item(Item={"batch_id": batch_id, "record_id": "META", "operation": operation,
                             "status": "WAITING_FOR_BATCH", "created_at": datetime.now(timezone.utc).isoformat()},
                       ConditionExpression="attribute_not_exists(batch_id)")
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        meta = table.get_item(Key={"batch_id": batch_id, "record_id": "META"}, ConsistentRead=True)["Item"]
        if meta["operation"] != operation or meta["status"] != "WAITING_FOR_BATCH":
            return response(409, {"code": "BATCH_CLOSED_OR_CONFLICT"})
    ingestion_id = str(uuid.uuid4())
    stamp = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    prefix = "_control" if dataset == FINALIZE else dataset
    key = f"ingestion/{prefix}/{source}/{stamp}/{ingestion_id}/{name}"
    metadata = {"ingestion-id": ingestion_id, "batch-id": batch_id, "dataset-type": dataset,
                "sha256": sha256, "source-system": source, "schema-version": schema_version,
                "expected-size": str(size_bytes)}
    try:
        url = s3.generate_presigned_url("put_object", Params={
            "Bucket": os.environ["BRONZE_BUCKET"], "Key": key, "ContentType": content_type,
            "Metadata": metadata, "ChecksumSHA256": base64.b64encode(bytes.fromhex(sha256)).decode(),
        }, ExpiresIn=900)
    except Exception:
        return response(503, {"code": "PRESIGN_UNAVAILABLE"})
    table.put_item(Item={"batch_id": batch_id, "record_id": f"UPLOAD#{ingestion_id}",
                         "dataset_type": dataset, "status": "PENDING", "s3_key": key,
                         "sha256": sha256, "size_bytes": size_bytes},
                   ConditionExpression="attribute_not_exists(record_id)")
    headers = {"Content-Type": content_type, "x-amz-checksum-sha256": base64.b64encode(bytes.fromhex(sha256)).decode()}
    headers.update({"x-amz-meta-" + k: v for k, v in metadata.items()})
    return response(201, {"ingestion_id": ingestion_id, "batch_id": batch_id,
                          "s3_key": key, "upload_url": url, "expires_in_seconds": 900,
                          "required_headers": headers})
