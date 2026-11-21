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
    # S3 aceita somente ASCII em metadados HTTP; o motivo completo continua no ledger.
    metadata_reason = reason.encode("ascii", "ignore").decode()
    s3.copy_object(Bucket=os.environ["QUARANTINE_BUCKET"], Key=target,
                   CopySource={"Bucket": bucket, "Key": key, "VersionId": version},
                   MetadataDirective="REPLACE", Metadata={"reason": metadata_reason[:180], "source-key": key})


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
    if metadata.get("batch-id") and metadata.get("ingestion-id"):
        boto3.resource("dynamodb").Table(os.environ["BATCH_TABLE"]).update_item(
            Key={"batch_id": metadata["batch-id"], "record_id": f"UPLOAD#{metadata['ingestion-id']}"},
            UpdateExpression="SET #s=:rejected, rejection_reason=:reason",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":rejected": "REJECTED", ":reason": reason[:500]})


def reject(bucket, key, version, metadata, reason):
    quarantine(bucket, key, version, reason)
    record_rejection(bucket, key, version, metadata, reason)
    raise PermanentIngestionError(reason)


def record_received(object_version, metadata):
    try:
        ddb.put_item(TableName=os.environ["LEDGER_TABLE"], Item={
            "object_version": {"S": object_version}, "ingestion_id": {"S": metadata["ingestion-id"]},
            "batch_id": {"S": metadata["batch-id"]}, "status": {"S": "RECEIVED"}},
            ConditionExpression="attribute_not_exists(object_version)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


def process(s3_event):
    bucket = s3_event["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(s3_event["s3"]["object"]["key"])
    version = s3_event["s3"]["object"].get("versionId")
    if bucket != os.environ["BRONZE_BUCKET"] or not version:
        raise ValueError("evento fora do Bronze versionado")
    head = s3.head_object(Bucket=bucket, Key=key, VersionId=version)
    metadata = head.get("Metadata", {})
    required = ["ingestion-id", "batch-id", "dataset-type", "sha256", "source-system", "schema-version", "expected-size"]
    if any(not metadata.get(field) for field in required):
        reject(bucket, key, version, metadata, "metadados obrigatórios ausentes")
    dataset = metadata["dataset-type"]
    if metadata["schema-version"] != "1":
        reject(bucket, key, version, metadata, "schema_version não suportado")
    if dataset not in {"pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"} or not key.startswith(f"ingestion/{dataset}/"):
        reject(bucket, key, version, metadata, "dataset_type incompatível")
    if not metadata["expected-size"].isdigit() or int(metadata["expected-size"]) != head.get("ContentLength"):
        reject(bucket, key, version, metadata, "tamanho do objeto divergente")
    actual = sha256_object(bucket, key, version)
    if actual != metadata["sha256"].lower():
        reject(bucket, key, version, metadata, "sha256 divergente")
    object_version = f"{bucket}/{key}/{version}"
    table = boto3.resource("dynamodb").Table(os.environ["BATCH_TABLE"])
    upload_key = {"batch_id": metadata["batch-id"], "record_id": f"UPLOAD#{metadata['ingestion-id']}"}
    if table.get_item(Key={"batch_id": metadata["batch-id"], "record_id": f"FILE#{object_version}"}, ConsistentRead=True).get("Item"):
        record_received(object_version, metadata)
        return
    upload = table.get_item(Key=upload_key, ConsistentRead=True).get("Item")
    if not upload or upload["s3_key"] != key or upload["sha256"] != actual:
        reject(bucket, key, version, metadata, "upload sem sessão válida")
    meta = table.get_item(Key={"batch_id": metadata["batch-id"], "record_id": "META"}, ConsistentRead=True).get("Item")
    if not meta or meta["status"] != "WAITING_FOR_BATCH":
        reject(bucket, key, version, metadata, "lote inexistente ou encerrado")
    table.put_item(Item={"batch_id": metadata["batch-id"], "record_id": f"FILE#{object_version}",
                         "ingestion_id": metadata["ingestion-id"], "dataset_type": dataset,
                         "source_system": metadata["source-system"], "schema_version": metadata["schema-version"],
                         "s3_key": key, "version_id": version, "sha256": actual,
                         "size_bytes": head["ContentLength"], "status": "RECEIVED"},
                   ConditionExpression="attribute_not_exists(record_id)")
    table.update_item(Key=upload_key, UpdateExpression="SET #s=:received",
                      ExpressionAttributeNames={"#s": "status"},
                      ExpressionAttributeValues={":received": "RECEIVED"})

    record_received(object_version, metadata)


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
            except Exception as exc:
                print(json.dumps({"event": "ingestion_retry", "message_id": record["messageId"],
                                  "error_type": type(exc).__name__, "reason": str(exc)}))
                retry_record = True
        if retry_record:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
