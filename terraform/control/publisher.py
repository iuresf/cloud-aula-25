"""Publica, por um único ponteiro DynamoDB, o conjunto validado de snapshots Gold."""
import json
import os
from datetime import datetime, timezone
from urllib.parse import urlparse
import boto3

s3 = boto3.client("s3")
glue = boto3.client("glue")
ddb = boto3.client("dynamodb")
GOLD = ("dim_tempo", "dim_cliente", "dim_produto", "dim_canal", "dim_regiao", "fato_itens_venda", "fato_sessoes")
SILVER = ("pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos")


def manifest(key):
    return json.loads(s3.get_object(Bucket=os.environ["RESULTS_BUCKET"], Key=key)["Body"].read())


def validate(run_id):
    gold = manifest(f"manifestos/gold/{run_id}.json")
    if gold.get("status") != "CANDIDATE" or gold.get("run_id") != run_id:
        raise ValueError("candidata Gold inválida")
    uri = urlparse(gold["silver_operational_manifest"])
    if uri.scheme != "s3" or uri.netloc != os.environ["RESULTS_BUCKET"]:
        raise ValueError("manifesto Silver fora do bucket de resultados")
    silver = manifest(uri.path.lstrip("/"))
    if silver.get("run_id") != gold.get("source_silver_run_id") or silver.get("status") != "SUCCEEDED":
        raise ValueError("linhagem Silver inválida")
    for name in GOLD:
        if not gold.get("snapshot_ids", {}).get(name):
            raise ValueError(f"snapshot Gold ausente: {name}")
        table = glue.get_table(DatabaseName=os.environ["DATABASE"], Name=name)["Table"]
        if table.get("Parameters", {}).get("metadata_location") != gold["iceberg_metadata_locations"].get(name):
            raise ValueError(f"tabela Gold mudou durante validação: {name}")
    for name in SILVER:
        if not silver.get("snapshot_ids", {}).get(name):
            raise ValueError(f"snapshot Silver ausente: {name}")
    # Política explícita: manifestos legados continuam legíveis para consulta,
    # porém não podem avançar PUBLISHED sem reconciliação verificável.
    if gold.get("reconciliation_schema_version") != "1" or gold.get("reconciliation_status") != "PASS":
        raise ValueError("manifesto Gold sem reconciliação compatível")
    report_uri = urlparse(gold.get("reconciliation_uri", ""))
    if report_uri.scheme != "s3" or report_uri.netloc != os.environ["RESULTS_BUCKET"]:
        raise ValueError("relatório de reconciliação fora do bucket de resultados")
    report = manifest(report_uri.path.lstrip("/"))
    if (report.get("run_id") != run_id or str(report.get("attempt_id")) != str(gold.get("attempt_id"))
            or report.get("source_silver_run_id") != gold.get("source_silver_run_id")
            or report.get("silver_snapshot_ids") != silver.get("snapshot_ids")
            or report.get("gold_snapshot_ids") != gold.get("snapshot_ids")
            or report.get("status") != "PASS"):
        raise ValueError("relatório de reconciliação incompatível")
    checks = report.get("checks")
    if not isinstance(checks, list) or not checks or any(check.get("status") != "PASS" or not check.get("id") for check in checks):
        raise ValueError("reconciliação Gold sem verificações aprovadas")
    return gold, silver


def handler(event, _context):
    run_id = event["run_id"]
    gold, silver = validate(run_id)
    table = os.environ["OPERATIONS_TABLE"]
    pointer = ddb.get_item(TableName=table, Key={"run_id": {"S": "PUBLISHED"}}, ConsistentRead=True).get("Item", {})
    previous = pointer.get("published_run_id", {}).get("S")
    values = {":run": {"S": run_id}, ":batch": {"S": gold["batch_id"]},
              ":gold": {"S": f"manifestos/gold/{run_id}.json"},
              ":silver": {"S": gold["silver_operational_manifest"]},
              ":snapshots": {"S": json.dumps(gold["snapshot_ids"])},
              ":now": {"S": datetime.now(timezone.utc).isoformat()}}
    condition = "attribute_not_exists(run_id)" if not previous else "published_run_id=:previous"
    if previous:
        values[":previous"] = {"S": previous}
    ddb.update_item(TableName=table, Key={"run_id": {"S": "PUBLISHED"}},
                    UpdateExpression="SET published_run_id=:run, batch_id=:batch, gold_manifest=:gold, silver_manifest=:silver, snapshot_ids=:snapshots, published_at=:now",
                    ConditionExpression=condition, ExpressionAttributeValues=values)
    try:
        ddb.delete_item(TableName=table, Key={"run_id": {"S": "LOCK"}},
                        ConditionExpression="#owner=:owner", ExpressionAttributeNames={"#owner": "owner"}, ExpressionAttributeValues={":owner": {"S": run_id}})
    except ddb.exceptions.ConditionalCheckFailedException:
        pass
    return {"run_id": run_id, "status": "PUBLISHED", "previous_run_id": previous}
