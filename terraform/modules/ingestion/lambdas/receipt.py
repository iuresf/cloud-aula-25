"""Valida cada objeto Bronze, cria o manifesto e entrega o lote à orquestração."""
import hashlib
import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
ddb = boto3.client("dynamodb")


class PermanentIngestionError(Exception):
    """Erro de contrato do arquivo: preserva evidência e confirma a mensagem SQS."""


def sha256_object(bucket, key, version):
    digest = hashlib.sha256()
    body = s3.get_object(Bucket=bucket, Key=key, VersionId=version)["Body"]
    for chunk in iter(lambda: body.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def quarantine(bucket, key, version, reason):
    target = f"rejected/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{key}"
    s3.copy_object(Bucket=os.environ["QUARANTINE_BUCKET"], Key=target,
                   CopySource={"Bucket": bucket, "Key": key, "VersionId": version},
                   MetadataDirective="REPLACE", Metadata={"reason": reason[:180], "source-key": key})


def record_rejection(bucket, key, version, metadata, reason):
    """Mantém no ledger o objeto rejeitado, mesmo quando não há manifesto válido."""
    object_version = f"{bucket}/{key}/{version}"
    ddb.update_item(
        TableName=os.environ["LEDGER_TABLE"], Key={"object_version": {"S": object_version}},
        UpdateExpression="SET #status=:status, rejection_reason=:reason, ingestion_id=:ingestion_id",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": {"S": "REJECTED"}, ":reason": {"S": reason[:500]},
            ":ingestion_id": {"S": metadata.get("ingestion-id", "unknown")},
        },
    )


def reject(bucket, key, version, metadata, reason):
    quarantine(bucket, key, version, reason)
    record_rejection(bucket, key, version, metadata, reason)
    raise PermanentIngestionError(reason)


def process(s3_event):
    bucket = s3_event["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(s3_event["s3"]["object"]["key"])
    version = s3_event["s3"]["object"].get("versionId")
    if bucket != os.environ["BRONZE_BUCKET"] or not version:
        raise ValueError("evento fora do Bronze versionado")
    head = s3.head_object(Bucket=bucket, Key=key, VersionId=version)
    metadata = head.get("Metadata", {})
    required = ["ingestion-id", "sha256", "source-system", "schema-version", "expected-size"]
    if any(not metadata.get(field) for field in required):
        reject(bucket, key, version, metadata, "metadados obrigatórios ausentes")
    if not metadata["expected-size"].isdigit() or int(metadata["expected-size"]) != head.get("ContentLength"):
        reject(bucket, key, version, metadata, "tamanho do objeto divergente")
    actual = sha256_object(bucket, key, version)
    if actual != metadata["sha256"].lower():
        reject(bucket, key, version, metadata, "sha256 divergente")
    object_version = f"{bucket}/{key}/{version}"
    manifest_key = f"manifestos/bronze/ingestion-{metadata['ingestion-id']}.json"
    manifest = {"layer": "bronze", "ingestion_id": metadata["ingestion-id"], "source_system": metadata["source-system"],
                "schema_version": metadata["schema-version"], "created_at": datetime.now(timezone.utc).isoformat(),
                "files": [{"s3_key": key, "version_id": version, "sha256": actual, "content_type": head.get("ContentType")}]} 
    s3.put_object(Bucket=os.environ["RESULTS_BUCKET"], Key=manifest_key, Body=json.dumps(manifest, indent=2).encode(),
                  ContentType="application/json", Metadata={"sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()})
    try:
        ddb.put_item(TableName=os.environ["LEDGER_TABLE"], Item={"object_version": {"S": object_version},
                     "ingestion_id": {"S": metadata["ingestion-id"]}, "manifest_key": {"S": manifest_key}, "status": {"S": "RECEIVED"}},
                     ConditionExpression="attribute_not_exists(object_version)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        existing = ddb.get_item(TableName=os.environ["LEDGER_TABLE"], Key={"object_version": {"S": object_version}}).get("Item", {})
        if existing.get("status", {}).get("S") == "ORCHESTRATED":
            return
    sqs.send_message(QueueUrl=os.environ["READY_QUEUE_URL"], MessageBody=json.dumps({"ingestion_id": metadata["ingestion-id"], "bronze_manifest": manifest_key, "object_version": object_version}))


def handler(event, _context):
    failures = []
    for record in event["Records"]:
        try:
            envelope = json.loads(record["body"])
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})
            continue
        retry_record = False
        for item in envelope.get("Records", []):
            try:
                process(item)
            except PermanentIngestionError as exc:
                print(json.dumps({"event": "ingestion_rejected", "message_id": record["messageId"], "reason": str(exc)}))
            except Exception:
                retry_record = True
        if retry_record:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
