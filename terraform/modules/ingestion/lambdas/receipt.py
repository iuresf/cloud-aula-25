"""Valida objetos Bronze; finalize.json fecha e dispara um lote."""
import hashlib
import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
ddb = boto3.client("dynamodb")
DATASETS = {"pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"}
FINALIZE = "batch_finalize"


class PermanentIngestionError(Exception): pass


def is_conditional(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def sha256_object(bucket, key, version):
    digest = hashlib.sha256(); body = s3.get_object(Bucket=bucket, Key=key, VersionId=version)["Body"]
    for chunk in iter(lambda: body.read(1024 * 1024), b""): digest.update(chunk)
    return digest.hexdigest()


def reject(bucket, key, version, metadata, reason):
    target=f"rejected/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{key}"
    s3.copy_object(Bucket=os.environ["QUARANTINE_BUCKET"],Key=target,CopySource={"Bucket":bucket,"Key":key,"VersionId":version},MetadataDirective="REPLACE",Metadata={"reason":reason.encode("ascii","ignore").decode()[:180],"source-key":key})
    ddb.update_item(TableName=os.environ["LEDGER_TABLE"],Key={"object_version":{"S":f"{bucket}/{key}/{version}"}},UpdateExpression="SET #s=:rejected, rejection_reason=:reason",ExpressionAttributeNames={"#s":"status"},ExpressionAttributeValues={":rejected":{"S":"REJECTED"},":reason":{"S":reason[:500]}})
    if metadata.get("batch-id") and metadata.get("ingestion-id"):
        boto3.resource("dynamodb").Table(os.environ["BATCH_TABLE"]).update_item(Key={"batch_id":metadata["batch-id"],"record_id":f"UPLOAD#{metadata['ingestion-id']}"},UpdateExpression="SET #s=:rejected",ExpressionAttributeNames={"#s":"status"},ExpressionAttributeValues={":rejected":"REJECTED"})
    raise PermanentIngestionError(reason)


def rows_for(table, batch_id):
    rows=[]; args={"KeyConditionExpression":Key("batch_id").eq(batch_id),"ConsistentRead":True}
    while True:
        page=table.query(**args); rows.extend(page["Items"])
        if "LastEvaluatedKey" not in page: return rows
        args["ExclusiveStartKey"]=page["LastEvaluatedKey"]


def declared_files(bucket, key, version, metadata, operation):
    try:
        payload=json.loads(s3.get_object(Bucket=bucket,Key=key,VersionId=version)["Body"].read())
        if payload.get("type") != "batch_finalize" or payload.get("batch_id") != metadata["batch-id"] or payload.get("operation") != operation: raise ValueError("identidade do lote divergente")
        files=[]
        for item in payload["files"]:
            digest=item["sha256"].lower()
            if item["dataset_type"] not in DATASETS or not isinstance(item["s3_key"],str) or not isinstance(item["size_bytes"],int) or len(digest)!=64 or any(c not in "0123456789abcdef" for c in digest): raise ValueError("arquivo declarado inválido")
            files.append({"dataset_type":item["dataset_type"],"s3_key":item["s3_key"],"sha256":digest,"size_bytes":item["size_bytes"]})
        if not files or len({(f["dataset_type"],f["s3_key"]) for f in files}) != len(files): raise ValueError("lista vazia ou duplicada")
        if operation == "initial" and {f["dataset_type"] for f in files} != DATASETS: raise ValueError("lote inicial incompleto")
        return files
    except (KeyError,TypeError,ValueError,json.JSONDecodeError) as exc:
        reject(bucket,key,version,metadata,f"finalize.json inválido: {exc}")


def complete_if_ready(table, batch_id):
    rows=rows_for(table,batch_id); meta=next((r for r in rows if r["record_id"]=="META"),None); controls=[r for r in rows if r["record_id"].startswith("FINALIZE#")]
    if not meta or meta["status"] not in ("WAITING_FOR_BATCH","READY") or not controls: return
    expected=controls[0]["files"]; data=[r for r in rows if r["record_id"].startswith("FILE#")]
    if any(r["status"]!="RECEIVED" for r in data): return
    actual={(r["dataset_type"],r["s3_key"],r["sha256"].lower(),int(r["size_bytes"])) for r in data}; declared={(r["dataset_type"],r["s3_key"],r["sha256"].lower(),int(r["size_bytes"])) for r in expected}
    if actual != declared: return
    manifest_key=f"manifestos/bronze/{batch_id}.json"
    manifest={"layer":"bronze","batch_id":batch_id,"operation":meta["operation"],"created_at":datetime.now(timezone.utc).isoformat(),"finalize_object":controls[0]["s3_key"],"files":[{k:(int(r[k]) if k=="size_bytes" else r[k]) for k in ("ingestion_id","batch_id","dataset_type","source_system","schema_version","s3_key","version_id","sha256","size_bytes","status")} for r in sorted(data,key=lambda r:r["record_id"])]}
    if meta["status"] == "WAITING_FOR_BATCH":
        s3.put_object(Bucket=os.environ["RESULTS_BUCKET"],Key=manifest_key,Body=json.dumps(manifest).encode(),ContentType="application/json")
        try:
            table.update_item(Key={"batch_id":batch_id,"record_id":"META"},UpdateExpression="SET #s=:ready, bronze_manifest=:manifest",ConditionExpression="#s=:waiting",ExpressionAttributeNames={"#s":"status"},ExpressionAttributeValues={":ready":"READY",":waiting":"WAITING_FOR_BATCH",":manifest":manifest_key})
        except Exception as exc:
            if not is_conditional(exc): raise
    if table.get_item(Key={"batch_id":batch_id,"record_id":"META"},ConsistentRead=True).get("Item",{}).get("status") == "READY":
        sqs.send_message(QueueUrl=os.environ["READY_QUEUE_URL"],MessageBody=json.dumps({"batch_id":batch_id,"bronze_manifest":manifest_key}))


def process(event):
    bucket=event["s3"]["bucket"]["name"]; key=urllib.parse.unquote_plus(event["s3"]["object"]["key"]); version=event["s3"]["object"].get("versionId")
    if bucket != os.environ["BRONZE_BUCKET"] or not version: raise ValueError("evento fora do Bronze versionado")
    head=s3.head_object(Bucket=bucket,Key=key,VersionId=version); metadata=head.get("Metadata",{}); required=["ingestion-id","batch-id","dataset-type","sha256","source-system","schema-version","expected-size"]
    if any(not metadata.get(k) for k in required): reject(bucket,key,version,metadata,"metadados obrigatórios ausentes")
    dataset=metadata["dataset-type"]; valid=(dataset in DATASETS and key.startswith(f"ingestion/{dataset}/")) or (dataset == FINALIZE and key.startswith("ingestion/_control/"))
    if metadata["schema-version"]!="1" or not valid: reject(bucket,key,version,metadata,"dataset_type incompatível")
    if not metadata["expected-size"].isdigit() or int(metadata["expected-size"]) != head.get("ContentLength"): reject(bucket,key,version,metadata,"tamanho do objeto divergente")
    actual=sha256_object(bucket,key,version)
    if actual != metadata["sha256"].lower(): reject(bucket,key,version,metadata,"sha256 divergente")
    table=boto3.resource("dynamodb").Table(os.environ["BATCH_TABLE"]); batch=metadata["batch-id"]; upload_key={"batch_id":batch,"record_id":f"UPLOAD#{metadata['ingestion-id']}"}; upload=table.get_item(Key=upload_key,ConsistentRead=True).get("Item"); meta=table.get_item(Key={"batch_id":batch,"record_id":"META"},ConsistentRead=True).get("Item")
    if not upload or upload["s3_key"] != key or upload["sha256"] != actual or not meta or meta["status"] not in ("WAITING_FOR_BATCH","READY"): reject(bucket,key,version,metadata,"upload sem sessão válida ou lote encerrado")
    object_version=f"{bucket}/{key}/{version}"
    try:
        if dataset == FINALIZE:
            table.put_item(Item={"batch_id":batch,"record_id":f"FINALIZE#{object_version}","s3_key":key,"version_id":version,"files":declared_files(bucket,key,version,metadata,meta["operation"]),"status":"RECEIVED"},ConditionExpression="attribute_not_exists(record_id)")
        else:
            table.put_item(Item={"batch_id":batch,"record_id":f"FILE#{object_version}","ingestion_id":metadata["ingestion-id"],"dataset_type":dataset,"source_system":metadata["source-system"],"schema_version":metadata["schema-version"],"s3_key":key,"version_id":version,"sha256":actual,"size_bytes":head["ContentLength"],"status":"RECEIVED"},ConditionExpression="attribute_not_exists(record_id)")
    except Exception as exc:
        if not is_conditional(exc): raise
    table.update_item(Key=upload_key,UpdateExpression="SET #s=:received",ExpressionAttributeNames={"#s":"status"},ExpressionAttributeValues={":received":"RECEIVED"})
    try: ddb.put_item(TableName=os.environ["LEDGER_TABLE"],Item={"object_version":{"S":object_version},"ingestion_id":{"S":metadata["ingestion-id"]},"batch_id":{"S":batch},"status":{"S":"RECEIVED"}},ConditionExpression="attribute_not_exists(object_version)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException": raise
    complete_if_ready(table,batch)


def handler(event, _context):
    failures=[]
    for record in event["Records"]:
        try: envelope=json.loads(record["body"])
        except Exception: failures.append({"itemIdentifier":record["messageId"]}); continue
        retry=False
        for item in envelope.get("Records",[]):
            try: process(item)
            except PermanentIngestionError as exc: print(json.dumps({"event":"ingestion_rejected","reason":str(exc)}))
            except Exception as exc: print(json.dumps({"event":"ingestion_retry","reason":str(exc)})); retry=True
        if retry: failures.append({"itemIdentifier":record["messageId"]})
    return {"batchItemFailures":failures}
