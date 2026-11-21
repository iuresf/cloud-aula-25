"""Publicação validada por ponteiro único, com clientes AWS falsos."""
import importlib.util
import io
import json
import os
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
os.environ.update(RESULTS_BUCKET="results", DATABASE="lab", OPERATIONS_TABLE="ops")


class S3:
    def __init__(self): self.gold = {}; self.silver = {}
    def get_object(self, Bucket, Key):
        assert Bucket == "results"
        data = self.gold if Key.startswith("manifestos/gold/") else self.silver
        return {"Body": io.BytesIO(json.dumps(data).encode())}


class Glue:
    def __init__(self): self.changed = None
    def get_table(self, DatabaseName, Name):
        return {"Table": {"Parameters": {"metadata_location": "changed" if Name == self.changed else f"s3://gold/{Name}/metadata.json"}}}


class DDB:
    def __init__(self): self.pointer = None; self.writes = []
    def get_item(self, **_kwargs):
        return {"Item": {"published_run_id": {"S": self.pointer}}} if self.pointer else {}
    def delete_item(self, **_kwargs): pass
    def update_item(self, **kwargs):
        self.writes.append(kwargs)
        if kwargs["Key"]["run_id"]["S"] == "PUBLISHED":
            self.pointer = kwargs["ExpressionAttributeValues"][":run"]["S"]


s3, glue, ddb = S3(), Glue(), DDB()
boto = types.ModuleType("boto3")
boto.client = lambda name: {"s3": s3, "glue": glue, "dynamodb": ddb}[name]
sys.modules["boto3"] = boto
spec = importlib.util.spec_from_file_location("publisher", ROOT / "terraform/control/publisher.py")
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


@pytest.fixture(autouse=True)
def setup():
    ddb.pointer = "previous"; ddb.writes.clear(); glue.changed = None
    s3.gold = {"status": "CANDIDATE", "run_id": "new", "batch_id": "batch_1", "source_silver_run_id": "original",
               "silver_operational_manifest": "s3://results/manifestos/silver/original.json",
               "snapshot_ids": {name: str(i) for i, name in enumerate(publisher.GOLD, 1)},
               "iceberg_metadata_locations": {name: f"s3://gold/{name}/metadata.json" for name in publisher.GOLD}}
    s3.silver = {"run_id": "original", "status": "SUCCEEDED",
                 "snapshot_ids": {name: str(i) for i, name in enumerate(publisher.SILVER, 1)}}


def test_gold_recovery_publishes_original_silver_lineage():
    result = publisher.handler({"run_id": "new"}, None)
    assert result == {"run_id": "new", "status": "PUBLISHED", "previous_run_id": "previous"}
    assert ddb.pointer == "new"
    assert len(ddb.writes) == 1


def test_failed_candidate_preserves_previous_pointer():
    glue.changed = "dim_cliente"
    with pytest.raises(ValueError, match="mudou"):
        publisher.handler({"run_id": "new"}, None)
    assert ddb.pointer == "previous" and not ddb.writes


def test_missing_silver_snapshot_prevents_publication():
    del s3.silver["snapshot_ids"]["pedidos"]
    with pytest.raises(ValueError, match="Silver ausente"):
        publisher.handler({"run_id": "new"}, None)
    assert ddb.pointer == "previous"


def test_partial_gold_commit_without_seven_snapshots_does_not_publish():
    del s3.gold["snapshot_ids"]["fato_sessoes"]
    with pytest.raises(ValueError, match="snapshot Gold ausente"):
        publisher.handler({"run_id": "new"}, None)
    assert ddb.pointer == "previous" and not ddb.writes
