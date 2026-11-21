"""Contratos locais da Lambda de recebimento; não chama serviços AWS."""
import hashlib
import importlib.util
import json
import os
import sys
import types
from pathlib import Path


os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["BRONZE_BUCKET"] = "bronze-test"
sys.modules["boto3"] = types.SimpleNamespace(client=lambda _service: object())
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("api_ingestion", ROOT / "terraform/modules/ingestion/lambdas/api_ingestion.py")
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


class S3Stub:
    def generate_presigned_url(self, operation, Params, ExpiresIn):
        self.operation, self.params, self.expires = operation, Params, ExpiresIn
        return "https://example.invalid/upload"


def test_api_returns_checksum_and_trace_metadata(monkeypatch):
    stub = S3Stub()
    monkeypatch.setattr(api, "s3", stub)
    digest = hashlib.sha256(b"pedido;P1\n").hexdigest()
    event = {"body": json.dumps({"file_name": "pedidos.csv", "content_type": "text/csv",
                                   "source_system": "erp", "schema_version": "1", "sha256": digest})}
    result = api.handler(event, None)
    body = json.loads(result["body"])
    assert result["statusCode"] == 201
    assert body["required_headers"]["x-amz-checksum-sha256"]
    assert stub.params["Metadata"]["sha256"] == digest
    assert stub.params["Key"].startswith("ingestion/erp/")


def test_api_rejects_invalid_hash(monkeypatch):
    monkeypatch.setattr(api, "s3", S3Stub())
    result = api.handler({"body": json.dumps({"file_name": "pedidos.csv", "content_type": "text/csv",
                                                "source_system": "erp", "schema_version": "1", "sha256": "bad"})}, None)
    assert result["statusCode"] == 400
