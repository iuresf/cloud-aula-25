"""Testa a função de leitura versionada do job Glue sem importar Spark."""
import ast
import hashlib
import io
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
tree = ast.parse((ROOT / "terraform/jobs/silver.py").read_text())
fn = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "stage_manifest_objects")
namespace = {"hashlib": hashlib, "re": re,
             "DATASETS": {"pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"}}
exec(compile(ast.Module(body=[fn], type_ignores=[]), "silver.py", "exec"), namespace)
stage = namespace["stage_manifest_objects"]


class VersionedS3:
    def __init__(self):
        self.objects = {"old": b"pedido_id;status\nP1;pago", "new": b"pedido_id;status\nP1;cancelado"}
        self.reads = []
        self.staged = []
    def get_object(self, Bucket, Key, VersionId):
        self.reads.append((Bucket, Key, VersionId))
        return {"VersionId": VersionId, "Body": io.BytesIO(self.objects[VersionId])}
    def put_object(self, **kwargs): self.staged.append(kwargs)


def test_stages_exact_manifest_version_even_when_newer_exists():
    s3 = VersionedS3()
    key = "pedidos/lote_001.csv"
    record = {"s3_key": key, "version_id": "old", "sha256": hashlib.sha256(s3.objects["old"]).hexdigest()}
    staged, origin = stage(s3, "bronze", "results", "run1", [record])
    assert s3.reads == [("bronze", key, "old")]
    assert s3.staged[0]["Body"] == s3.objects["old"]
    assert origin[staged["pedidos"][0]] == f"s3://bronze/{key}?versionId=old"


def test_rejects_hash_mismatch_before_staging():
    s3 = VersionedS3()
    with pytest.raises(ValueError, match="SHA-256"):
        stage(s3, "bronze", "results", "run1", [{"s3_key": "pedidos/x.csv", "version_id": "old", "sha256": "0"*64}])
    assert not s3.staged


def test_rejects_duplicate_version():
    s3 = VersionedS3()
    record = {"s3_key": "pedidos/x.csv", "version_id": "old", "sha256": hashlib.sha256(s3.objects["old"]).hexdigest()}
    with pytest.raises(ValueError, match="duplicado"):
        stage(s3, "bronze", "results", "run1", [record, record])
