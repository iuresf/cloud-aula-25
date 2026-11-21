"""Cria uploads versionados e finaliza explicitamente um lote completo."""
import base64
import json
import os
import re
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")
glue = boto3.client("glue")
DATASETS = {"pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"}
ALLOWED = {"text/csv", "application/json", "application/x-ndjson"}


def response(status, body):
    return {"statusCode": status, "headers": {"content-type": "application/json"}, "body": json.dumps(body)}


def handler(event, _context):
    try:
        raw = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw).decode()
        payload = json.loads(raw)
        if payload.get("action") == "finalize":
            return finalize(payload)
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
    if dataset not in DATASETS or operation not in ("initial", "incremental"):
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
    key = f"ingestion/{dataset}/{source}/{stamp}/{ingestion_id}/{name}"
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


def finalize(payload):
    batch_id = payload["batch_id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{2,64}", batch_id):
        raise ValueError("batch_id inválido")
    table = ddb.Table(os.environ["BATCH_TABLE"])
    meta = table.get_item(Key={"batch_id": batch_id, "record_id": "META"}, ConsistentRead=True).get("Item")
    if not meta:
        return response(404, {"code": "BATCH_NOT_FOUND"})
    if meta["status"] not in ("WAITING_FOR_BATCH", "READY"):
        return response(409, {"code": "BATCH_CLOSED"})
    rows = []
    query_args = {"KeyConditionExpression": Key("batch_id").eq(batch_id), "ConsistentRead": True}
    while True:
        page = table.query(**query_args)
        rows.extend(page["Items"])
        if "LastEvaluatedKey" not in page: break
        query_args["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    if any(row["status"] != "RECEIVED" for row in rows if row["record_id"].startswith("UPLOAD#")):
        return response(409, {"code": "BATCH_UPLOAD_PENDING_OR_REJECTED"})
    files = [row for row in rows if row["record_id"].startswith("FILE#")]
    if not files or any(row["status"] != "RECEIVED" for row in files):
        return response(409, {"code": "BATCH_NOT_READY"})
    present = {row["dataset_type"] for row in files}
    if meta["operation"] == "initial" and present != DATASETS:
        return response(409, {"code": "BATCH_INCOMPLETE", "missing": sorted(DATASETS - present)})
    if meta["operation"] == "incremental":
        names = {"historico_pagamentos" if name == "pagamentos" else name for name in DATASETS - present}
        try:
            for name in names:
                glue.get_table(DatabaseName=os.environ["DATABASE"], Name=name)
        except glue.exceptions.EntityNotFoundException:
            return response(409, {"code": "SILVER_STATE_MISSING", "table": name})
    manifest_key = f"manifestos/bronze/{batch_id}.json"
    manifest = {"layer": "bronze", "batch_id": batch_id, "operation": meta["operation"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "files": [{key: (int(row[key]) if key == "size_bytes" else row[key]) for key in ("ingestion_id", "batch_id", "dataset_type", "source_system", "schema_version", "s3_key", "version_id", "sha256", "size_bytes", "status")} for row in sorted(files, key=lambda r: r["record_id"])]}
    if meta["status"] == "WAITING_FOR_BATCH":
        s3.put_object(Bucket=os.environ["RESULTS_BUCKET"], Key=manifest_key,
                      Body=json.dumps(manifest).encode(), ContentType="application/json")
        table.update_item(Key={"batch_id": batch_id, "record_id": "META"},
                          UpdateExpression="SET #s=:ready, bronze_manifest=:manifest",
                          ConditionExpression="#s=:waiting", ExpressionAttributeNames={"#s": "status"},
                          ExpressionAttributeValues={":ready": "READY", ":waiting": "WAITING_FOR_BATCH", ":manifest": manifest_key})
    sqs.send_message(QueueUrl=os.environ["READY_QUEUE_URL"], MessageBody=json.dumps({"batch_id": batch_id, "bronze_manifest": manifest_key}))
    return response(202, {"batch_id": batch_id, "status": "READY", "bronze_manifest": manifest_key})
