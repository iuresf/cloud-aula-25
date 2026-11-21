"""Eventos de execução e reconciliação para o ledger operacional."""
import json
import os
from datetime import datetime, timezone
import boto3

sfn = boto3.client("stepfunctions")
ddb = boto3.client("dynamodb")
TERMINAL = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}


def record(arn):
    detail = sfn.describe_execution(executionArn=arn)
    payload = json.loads(detail.get("input", "{}"))
    run_id = payload.get("run_id")
    if not run_id:
        return
    status = detail["status"]
    if status == "SUCCEEDED":
        pointer = ddb.get_item(TableName=os.environ["OPERATIONS_TABLE"],
                               Key={"run_id": {"S": "PUBLISHED"}}, ConsistentRead=True).get("Item", {})
        if pointer.get("published_run_id", {}).get("S") == run_id:
            status = "PUBLISHED"
    history = sfn.get_execution_history(executionArn=arn, reverseOrder=True, maxResults=20).get("events", []) if status not in ("SUCCEEDED", "PUBLISHED") else []
    failed_stage = "UNKNOWN"
    glue_job_run_id = None
    if detail.get("output"):
        try:
            outcome = json.loads(detail["output"])
            result = outcome.get("gold_result") or outcome.get("silver_result") or {}
            glue_job_run_id = result.get("JobRunId")
        except (ValueError, TypeError):
            pass
    for item in history:
        info = item.get("taskFailedEventDetails") or item.get("executionFailedEventDetails") or {}
        cause = json.dumps(info)
        submitted = item.get("taskSubmittedEventDetails", {}).get("output")
        if submitted and not glue_job_run_id:
            try: glue_job_run_id = json.loads(submitted).get("JobRunId")
            except (ValueError, TypeError): pass
        if "silver" in cause.lower(): failed_stage = "SILVER"; break
        if "gold" in cause.lower(): failed_stage = "GOLD"; break
    values = {":status": {"S": status}, ":arn": {"S": arn},
              ":batch": {"S": payload.get("batch_id", "")},
              ":attempt": {"N": str(payload.get("attempt_id", 1))},
              ":bronze": {"S": payload.get("glue_arguments", {}).get("--BRONZE_MANIFEST", "")},
              ":silver": {"S": f"manifestos/silver/{payload.get('glue_arguments', {}).get('--SOURCE_SILVER_RUN_ID', run_id)}.json"},
              ":gold": {"S": f"manifestos/gold/{run_id}.json"},
              ":stage": {"S": failed_stage},
              ":now": {"S": datetime.now(timezone.utc).isoformat()},
              ":error_type": {"S": detail.get("error") or ""},
              ":error_message": {"S": (detail.get("cause") or "")[:1000]},
              ":glue": {"S": glue_job_run_id or ""}}
    if payload.get("recovery_of"):
        values[":recovery"] = {"S": payload["recovery_of"]}
    expression = "SET #s=:status, execution_arn=:arn, batch_id=:batch, attempt_id=:attempt, bronze_manifest=:bronze, silver_manifest=:silver, gold_manifest=:gold, failed_stage=:stage, error_type=:error_type, error_message=:error_message, glue_job_run_id=:glue, updated_at=:now"
    if ":recovery" in values: expression += ", recovery_of=:recovery"
    values.update({":succeeded": {"S": "SUCCEEDED"}, ":failed": {"S": "FAILED"},
                   ":timed": {"S": "TIMED_OUT"}, ":aborted": {"S": "ABORTED"}, ":published": {"S": "PUBLISHED"}})
    ddb.update_item(TableName=os.environ["OPERATIONS_TABLE"], Key={"run_id": {"S": run_id}},
                    UpdateExpression=expression,
                    ConditionExpression="attribute_not_exists(#s) OR (#s <> :succeeded AND #s <> :failed AND #s <> :timed AND #s <> :aborted AND #s <> :published)",
                    ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues=values)

    batch_status = ("FAILED_" + failed_stage) if status == "FAILED" and failed_stage in ("SILVER", "GOLD") else status
    if status == "ABORTED": batch_status = "CANCELLED"
    if status == "SUCCEEDED": batch_status = "SUCCEEDED"
    if status == "PUBLISHED": batch_status = "PUBLISHED"
    try:
        ddb.update_item(TableName=os.environ["BATCH_TABLE"],
                        Key={"batch_id": {"S": payload.get("batch_id", "")}, "record_id": {"S": "META"}},
                        UpdateExpression="SET #s=:status", ConditionExpression="attribute_exists(batch_id)",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":status": {"S": batch_status}})
    except ddb.exceptions.ConditionalCheckFailedException:
        pass
    try:
        ddb.delete_item(TableName=os.environ["OPERATIONS_TABLE"], Key={"run_id": {"S": "LOCK"}},
                        ConditionExpression="#owner=:owner", ExpressionAttributeNames={"#owner": "owner"}, ExpressionAttributeValues={":owner": {"S": run_id}})
    except ddb.exceptions.ConditionalCheckFailedException:
        pass


def handler(event, _context):
    if event.get("source") == "aws.states":
        try: record(event["detail"]["executionArn"])
        except ddb.exceptions.ConditionalCheckFailedException: pass
    else:
        for page in sfn.get_paginator("list_executions").paginate(stateMachineArn=os.environ["STATE_MACHINE_ARN"]):
            for execution in page["executions"]:
                if execution["status"] in TERMINAL:
                    try: record(execution["executionArn"])
                    except ddb.exceptions.ConditionalCheckFailedException: pass
    return {"ok": True}
