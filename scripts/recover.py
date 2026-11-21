"""Inspeção, recuperação seletiva e consulta da versão analítica publicada."""
import argparse
import json
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
TABLES = ("dim_tempo", "dim_cliente", "dim_produto", "dim_canal", "dim_regiao", "fato_itens_venda", "fato_sessoes")


def outputs():
    raw = json.loads(subprocess.check_output(["terraform", "output", "-json"], cwd=ROOT / "terraform"))
    return {key: value["value"] for key, value in raw.items()}


def get_manifest(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def operation(ddb, table, run_id):
    return ddb.get_item(TableName=table, Key={"run_id": {"S": run_id}}, ConsistentRead=True).get("Item", {})


def string(item, key):
    return item.get(key, {}).get("S")


def snapshot_exists(athena, database, workgroup, table, snapshot_id):
    sql = f'SELECT snapshot_id FROM "{table}$snapshots" WHERE snapshot_id = {int(snapshot_id)}'
    qid = athena.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": database}, WorkGroup=workgroup)["QueryExecutionId"]
    import time
    for _ in range(60):
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return len(athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]) > 1
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena: {state} para snapshot {table}")
        time.sleep(2)
    raise TimeoutError(qid)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("inspect", "resume"):
        command = sub.add_parser(name)
        command.add_argument("--run-id", required=True)
        if name == "resume":
            command.add_argument("--from", dest="stage", choices=("SILVER", "GOLD"), required=True)
            command.add_argument("--bronze-manifest", help="Obrigatório para corrigir uma falha Silver")
    published = sub.add_parser("published")
    published.add_argument("--table", choices=TABLES)
    rollback = sub.add_parser("rollback")
    rollback.add_argument("--run-id", required=True)
    args = parser.parse_args()
    config = outputs()
    session = boto3.Session(region_name=config["region"])
    ddb, s3, sfn, athena = (session.client(name) for name in ("dynamodb", "s3", "stepfunctions", "athena"))
    table = config["operations_table"]
    bucket = config["buckets"]["results"]
    if args.command == "inspect":
        item = operation(ddb, table, args.run_id)
        if not item: raise ValueError("run_id não encontrado")
        print(json.dumps({key: next(iter(value.values())) for key, value in item.items()}, indent=2))
        return
    if args.command == "published":
        item = operation(ddb, table, "PUBLISHED")
        if not item: raise ValueError("nenhuma versão publicada")
        versions = json.loads(string(item, "snapshot_ids"))
        if args.table:
            print(f'SELECT * FROM "{args.table}" FOR VERSION AS OF {versions[args.table]};')
        else:
            print(json.dumps({"run_id": string(item, "published_run_id"), "snapshots": versions}, indent=2))
        return
    if args.command == "rollback":
        target = get_manifest(s3, bucket, f"manifestos/gold/{args.run_id}.json")
        versions = target["snapshot_ids"]
        for name in TABLES:
            if not snapshot_exists(athena, config["database"], config["workgroup"], name, versions[name]):
                raise ValueError(f"snapshot expirado: {name}={versions[name]}")
        current = operation(ddb, table, "PUBLISHED")
        previous = string(current, "published_run_id")
        if not previous: raise ValueError("nenhuma versão publicada para rollback")
        owner = f"rollback-{uuid.uuid4().hex}"
        ddb.put_item(TableName=table, Item={"run_id": {"S": "LOCK"}, "owner": {"S": owner}},
                     ConditionExpression="attribute_not_exists(run_id)")
        try:
            ddb.update_item(TableName=table, Key={"run_id": {"S": "PUBLISHED"}},
                UpdateExpression="SET published_run_id=:new, snapshot_ids=:snapshots, gold_manifest=:gold, silver_manifest=:silver, batch_id=:batch",
                ConditionExpression="published_run_id=:previous",
                ExpressionAttributeValues={":new": {"S": args.run_id}, ":snapshots": {"S": json.dumps(versions)},
                                           ":gold": {"S": f"manifestos/gold/{args.run_id}.json"},
                                           ":silver": {"S": target["silver_operational_manifest"]},
                                           ":batch": {"S": target["batch_id"]}, ":previous": {"S": previous}})
        finally:
            ddb.delete_item(TableName=table, Key={"run_id": {"S": "LOCK"}},
                            ConditionExpression="#owner=:owner", ExpressionAttributeNames={"#owner": "owner"}, ExpressionAttributeValues={":owner": {"S": owner}})
        print(json.dumps({"published_run_id": args.run_id, "previous_run_id": previous}))
        return
    old = operation(ddb, table, args.run_id)
    if not old or string(old, "status") not in ("FAILED", "TIMED_OUT", "ABORTED"):
        raise ValueError("execução não está em estado recuperável")
    failed_stage = string(old, "failed_stage")
    if failed_stage not in (args.stage, "UNKNOWN"):
        raise ValueError(f"etapa incompatível com falha: {failed_stage}")
    if args.stage == "SILVER" and not args.bronze_manifest:
        raise ValueError("correção Silver requer --bronze-manifest com novas versões")
    bronze_key = args.bronze_manifest or string(old, "bronze_manifest")
    original = get_manifest(s3, bucket, bronze_key)
    if original["batch_id"] != string(old, "batch_id"):
        raise ValueError("manifesto corrigido pertence a outro lote")
    source_silver = None
    if args.stage == "GOLD":
        uri = urlparse(string(old, "silver_manifest"))
        silver_key = uri.path.lstrip("/") if uri.scheme == "s3" else string(old, "silver_manifest")
        silver = get_manifest(s3, bucket, silver_key)
        source_silver = silver["run_id"]
        for name in ("pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"):
            if not snapshot_exists(athena, config["database"], config["workgroup"], name, silver["snapshot_ids"][name]):
                raise ValueError(f"snapshot Silver expirado: {name}")
    new_run = uuid.uuid4().hex
    payload_attempt = int(old.get("attempt_id", {}).get("N", "1")) + 1
    args_glue = {"--RUN_ID": new_run, "--BRONZE_MANIFEST": bronze_key, "--ATTEMPT_ID": str(payload_attempt)}
    if source_silver: args_glue["--SOURCE_SILVER_RUN_ID"] = source_silver
    payload = {"batch_id": original["batch_id"], "run_id": new_run,
               "attempt_id": payload_attempt,
               "recovery_of": args.run_id, "start_from": args.stage, "glue_arguments": args_glue}
    ddb.put_item(TableName=table, Item={"run_id": {"S": "LOCK"}, "owner": {"S": new_run}},
                 ConditionExpression="attribute_not_exists(run_id)")
    try:
        ddb.update_item(TableName=table, Key={"run_id": {"S": args.run_id}},
                        UpdateExpression="SET recovery_run_id=:new",
                        ConditionExpression="attribute_not_exists(recovery_run_id)",
                        ExpressionAttributeValues={":new": {"S": new_run}})
    except Exception:
        ddb.delete_item(TableName=table, Key={"run_id": {"S": "LOCK"}},
                        ConditionExpression="#owner=:owner", ExpressionAttributeNames={"#owner": "owner"}, ExpressionAttributeValues={":owner": {"S": new_run}})
        raise
    try:
        started = sfn.start_execution(stateMachineArn=config["state_machine_arn"], name=f"recovery-{new_run}", input=json.dumps(payload))
    except Exception:
        ddb.delete_item(TableName=table, Key={"run_id": {"S": "LOCK"}},
                        ConditionExpression="#owner=:owner", ExpressionAttributeNames={"#owner": "owner"}, ExpressionAttributeValues={":owner": {"S": new_run}})
        ddb.update_item(TableName=table, Key={"run_id": {"S": args.run_id}},
                        UpdateExpression="REMOVE recovery_run_id", ConditionExpression="recovery_run_id=:new",
                        ExpressionAttributeValues={":new": {"S": new_run}})
        raise
    print(json.dumps({"run_id": new_run, "recovery_of": args.run_id, "execution_arn": started["executionArn"]}, indent=2))


if __name__ == "__main__":
    main()
