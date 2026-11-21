"""Inicia exatamente uma execução por lote finalizado."""
import json
import os
import re
import uuid
import boto3
from botocore.exceptions import ClientError

sfn = boto3.client("stepfunctions")
ddb = boto3.resource("dynamodb")
locks = boto3.client("dynamodb")


def handler(event, _context):
    failures = []
    table = ddb.Table(os.environ["BATCH_TABLE"])
    for record in event["Records"]:
        try:
            message = json.loads(record["body"])
            batch_id = message["batch_id"]
            if not re.fullmatch(r"[A-Za-z0-9_-]{2,64}", batch_id):
                raise ValueError("batch_id inválido")
            meta = table.get_item(Key={"batch_id": batch_id, "record_id": "META"}, ConsistentRead=True)["Item"]
            if meta["status"] not in ("READY", "PROCESSING_SILVER") or meta["bronze_manifest"] != message["bronze_manifest"]:
                raise ValueError("lote não finalizado ou manifesto divergente")
            run_id = uuid.uuid5(uuid.NAMESPACE_URL, batch_id).hex
            arguments = {"--RUN_ID": run_id, "--BRONZE_MANIFEST": message["bronze_manifest"], "--ATTEMPT_ID": "1"}
            name = f"batch-{run_id}"
            acquired = False
            try:
                locks.put_item(TableName=os.environ["OPERATIONS_TABLE"],
                               Item={"run_id": {"S": "LOCK"}, "owner": {"S": run_id}},
                               ConditionExpression="attribute_not_exists(run_id)")
                acquired = True
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ConditionalCheckFailedException": raise
                held = locks.get_item(TableName=os.environ["OPERATIONS_TABLE"], Key={"run_id": {"S": "LOCK"}}, ConsistentRead=True).get("Item", {})
                if held.get("owner", {}).get("S") != run_id: raise RuntimeError("outra execução possui o bloqueio ETL")
            try:
                started = sfn.start_execution(stateMachineArn=os.environ["STATE_MACHINE_ARN"], name=name,
                    input=json.dumps({"batch_id": batch_id, "run_id": run_id, "attempt_id": 1,
                                      "start_from": "SILVER", "glue_arguments": arguments}))
                arn = started["executionArn"]
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ExecutionAlreadyExists":
                    if acquired:
                        locks.delete_item(TableName=os.environ["OPERATIONS_TABLE"],
                                          Key={"run_id": {"S": "LOCK"}},
                                          ConditionExpression="#owner=:owner", ExpressionAttributeNames={"#owner": "owner"},
                                          ExpressionAttributeValues={":owner": {"S": run_id}})
                    raise
                arn = f"{os.environ['STATE_MACHINE_ARN'].replace(':stateMachine:', ':execution:')}:{name}"
                if acquired:
                    locks.delete_item(TableName=os.environ["OPERATIONS_TABLE"],
                                      Key={"run_id": {"S": "LOCK"}},
                                      ConditionExpression="#owner=:owner", ExpressionAttributeNames={"#owner": "owner"},
                                      ExpressionAttributeValues={":owner": {"S": run_id}})
            except Exception:
                if acquired:
                    locks.delete_item(TableName=os.environ["OPERATIONS_TABLE"],
                                      Key={"run_id": {"S": "LOCK"}},
                                      ConditionExpression="#owner=:owner", ExpressionAttributeNames={"#owner": "owner"},
                                      ExpressionAttributeValues={":owner": {"S": run_id}})
                raise
            if meta["status"] == "READY":
                table.update_item(Key={"batch_id": batch_id, "record_id": "META"},
                                  UpdateExpression="SET #s=:processing, run_id=:run, execution_arn=:arn",
                                  ConditionExpression="#s=:ready", ExpressionAttributeNames={"#s": "status"},
                                  ExpressionAttributeValues={":processing": "PROCESSING_SILVER", ":ready": "READY", ":run": run_id, ":arn": arn})
        except Exception as exc:
            print(json.dumps({"event": "orchestration_retry", "message_id": record["messageId"], "reason": str(exc)}))
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
