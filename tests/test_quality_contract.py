from lakehouse.quality import duplicate_decision, record_hash
from lakehouse.reconciliation import report_matches


def test_identical_duplicates_ignore_provenance_and_choose_stable_winner():
    rows = [
        {"dataset_type": "pedidos", "pedido_id": "P1042", "status": "pago", "origem_arquivo": "s3://b/z", "source_position": 5},
        {"dataset_type": "pedidos", "pedido_id": "P1042", "status": "pago", "origem_arquivo": "s3://b/a", "source_position": 7},
    ]
    decisions = duplicate_decision(rows, "pedido_id", ("pedido_id", "status"))
    assert [item["category"] for item in decisions].count("DUPLICATE_IDENTICAL") == 1
    discarded = next(item for item in decisions if item["category"] == "DUPLICATE_IDENTICAL")
    assert discarded["kept_occurrence_id"] != discarded["row"]["occurrence_id"]


def test_conflicting_business_values_are_not_silently_selected():
    rows = [
        {"dataset_type": "pedidos", "pedido_id": "P1", "status": "pago", "origem_arquivo": "s3://b/a", "source_position": 2},
        {"dataset_type": "pedidos", "pedido_id": "P1", "status": "cancelado", "origem_arquivo": "s3://b/b", "source_position": 2},
    ]
    decisions = duplicate_decision(rows, "pedido_id", ("pedido_id", "status"))
    assert {item["category"] for item in decisions} == {"KEY_CONFLICT"}


def test_record_hash_is_canonical():
    assert record_hash({"b": 2, "a": 1}) == record_hash({"a": 1, "b": 2})


def test_reconciliation_requires_same_execution_and_passing_checks():
    report = {"run_id": "r", "attempt_id": 1, "source_silver_run_id": "s", "silver_snapshot_ids": {"a": "1"}, "gold_snapshot_ids": {"g": "2"}, "checks": [{"id": "revenue", "status": "PASS"}]}
    assert report_matches(report, "r", 1, "s", {"a": "1"}, {"g": "2"})
    report["checks"][0]["status"] = "FAIL"
    assert not report_matches(report, "r", 1, "s", {"a": "1"}, {"g": "2"})
