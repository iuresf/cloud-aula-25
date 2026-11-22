"""Validação local do vínculo entre relatório Gold e manifestos."""


def mandatory_checks_pass(report):
    checks = report.get("checks")
    return isinstance(checks, list) and bool(checks) and all(item.get("status") == "PASS" and item.get("id") for item in checks)


def report_matches(report, run_id, attempt_id, source_silver_run_id, silver_snapshot_ids, gold_snapshot_ids):
    return (
        report.get("run_id") == run_id
        and int(report.get("attempt_id", -1)) == int(attempt_id)
        and report.get("source_silver_run_id") == source_silver_run_id
        and report.get("silver_snapshot_ids") == silver_snapshot_ids
        and report.get("gold_snapshot_ids") == gold_snapshot_ids
        and mandatory_checks_pass(report)
    )
