"""Cria uma sessão de ingestão autenticada e devolve uma URL S3 pré-assinada."""
import base64
import json
import os
import re
import uuid
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
ALLOWED = {"text/csv", "application/json", "application/x-ndjson"}


def response(status, body):
    return {"statusCode": status, "headers": {"content-type": "application/json"}, "body": json.dumps(body)}


def handler(event, _context):
    try:
        raw = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw).decode()
        payload = json.loads(raw)
        name = payload["file_name"]
        sha256 = payload["sha256"].lower()
        source = payload["source_system"].lower()
        schema_version = str(payload["schema_version"])
        content_type = payload["content_type"]
        size_bytes = payload["size_bytes"]
        if content_type not in ALLOWED or not re.fullmatch(r"[a-f0-9]{64}", sha256):
            raise ValueError("content_type ou sha256 inválido")
        if not re.fullmatch(r"[a-z0-9_-]{2,40}", source) or not re.fullmatch(r"[A-Za-z0-9._-]{1,180}", name):
            raise ValueError("source_system ou file_name inválido")
        if not re.fullmatch(r"[0-9]{1,20}", schema_version):
            raise ValueError("schema_version inválido")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or not 0 < size_bytes <= int(os.environ["MAX_UPLOAD_BYTES"]):
            raise ValueError("size_bytes inválido ou acima do limite")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return response(400, {"code": "VALIDATION_ERROR", "message": str(exc)})

    ingestion_id = str(uuid.uuid4())
    stamp = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"ingestion/{source}/{stamp}/{ingestion_id}/{name}"
    metadata = {"ingestion-id": ingestion_id, "sha256": sha256, "source-system": source,
                "schema-version": schema_version, "expected-size": str(size_bytes)}
    try:
        url = s3.generate_presigned_url("put_object", Params={
            "Bucket": os.environ["BRONZE_BUCKET"], "Key": key, "ContentType": content_type, "Metadata": metadata,
            "ChecksumSHA256": base64.b64encode(bytes.fromhex(sha256)).decode(),
        }, ExpiresIn=900)
    except Exception:
        return response(503, {"code": "PRESIGN_UNAVAILABLE", "message": "não foi possível abrir a sessão de upload"})
    return response(201, {"ingestion_id": ingestion_id, "s3_key": key, "upload_url": url,
                          "expires_in_seconds": 900, "required_headers": {"Content-Type": content_type,
                          "x-amz-checksum-sha256": base64.b64encode(bytes.fromhex(sha256)).decode(),
                          "x-amz-meta-ingestion-id": ingestion_id, "x-amz-meta-sha256": sha256,
                          "x-amz-meta-source-system": source, "x-amz-meta-schema-version": schema_version,
                          "x-amz-meta-expected-size": str(size_bytes)}})
