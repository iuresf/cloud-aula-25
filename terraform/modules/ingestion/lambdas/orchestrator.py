"""Inicia uma execução idempotente da Step Functions para o manifesto Bronze."""
import json
import os
import re

import boto3
from botocore.exceptions import ClientError

sfn = boto3.client("stepfunctions")
ddb = boto3.client("dynamodb")


def mark_retry(message, reason):
    """Expõe a última causa antes de devolver a mensagem à fila ready."""
    object_version = message.get("object_version")
    if not object_version:
        return
    ddb.update_item(
        TableName=os.environ["LEDGER_TABLE"], Key={"object_version": {"S": object_version}},
        UpdateExpression="SET #status=:status, orchestration_error=:reason",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": {"S": "ORCHESTRATION_RETRY"}, ":reason": {"S": reason[:500]}},
    )


def handler(event, _context):
    failures = []
    for record in event["Records"]:
        message = {}
        try:
            message = json.loads(record["body"])
            ingestion_id = message["ingestion_id"]
            if not re.fullmatch(r"[0-9a-f-]{36}", ingestion_id):
                raise ValueError("ingestion_id inválido")
            execution_arn = None
            try:
                started = sfn.start_execution(stateMachineArn=os.environ["STATE_MACHINE_ARN"], name=f"ing-{ingestion_id}",
                    input=json.dumps({"glue_arguments": {"--RUN_ID": ingestion_id.replace("-", ""), "--BRONZE_MANIFEST": message["bronze_manifest"]}}))
                execution_arn = started["executionArn"]
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ExecutionAlreadyExists":
                    raise
            values = {":status": {"S": "ORCHESTRATED"}}
            expression = "SET #status=:status"
            if execution_arn:
                values[":arn"] = {"S": execution_arn}
                expression += ", execution_arn=:arn"
            ddb.update_item(TableName=os.environ["LEDGER_TABLE"], Key={"object_version": {"S": message["object_version"]}},
                            UpdateExpression=expression, ExpressionAttributeNames={"#status": "status"},
                            ExpressionAttributeValues=values)
        except Exception as exc:
            try:
                mark_retry(message, str(exc))
            except Exception:
                pass
            print(json.dumps({"event": "orchestration_retry", "message_id": record["messageId"], "reason": str(exc)}))
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
