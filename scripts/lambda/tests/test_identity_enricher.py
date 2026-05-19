"""
lifesync-identity-enricher Lambda tests.

Install:
  pip install pytest boto3 "moto[s3]"
"""

import json
import os
import sys
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BUCKET = "lifesync-raw"
REGION = "ap-northeast-2"


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("PRIVATE_API_BASE_URL", "http://private-api.local")


@pytest.fixture
def aws_env(aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        yield s3


def _put_raw(s3, source, records, date_iso="2026-05-14"):
    body = json.dumps({"source": source, "records": records}, ensure_ascii=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{source}/dt={date_iso}/{source}_test.json",
        Body=body.encode("utf-8"),
    )


def _get_result(s3, source, date_iso="2026-05-14"):
    obj = s3.get_object(Bucket=BUCKET, Key=f"{source}/dt={date_iso}/{source}_test.json")
    return json.loads(obj["Body"].read())


def _api_payloads(identity_items=None, consent_items=None):
    identity_items = identity_items or []
    consent_items = consent_items or []

    def fake_api_get(path, params=None):
        if path == "/identity-map":
            return {"items": identity_items}
        if path == "/consent-snapshot":
            return {"items": consent_items}
        raise AssertionError(f"unexpected path: {path}")

    return fake_api_get


class TestPrivateApiHelpers:
    def test_extract_items_accepts_items_wrapper(self):
        from lifesync_identity_enricher import _extract_items

        assert _extract_items({"items": [{"a": 1}]}) == [{"a": 1}]

    def test_extract_items_accepts_list(self):
        from lifesync_identity_enricher import _extract_items

        assert _extract_items([{"a": 1}]) == [{"a": 1}]

    def test_private_api_base_url_required(self, monkeypatch):
        from lifesync_identity_enricher import _get_private_api_config

        monkeypatch.delenv("PRIVATE_API_BASE_URL", raising=False)
        with pytest.raises(RuntimeError, match="PRIVATE_API_BASE_URL"):
            _get_private_api_config()


class TestEnrichSource:
    @patch("lifesync_identity_enricher._api_get")
    def test_global_id_added(self, mock_api_get, aws_env):
        from lifesync_identity_enricher import _enrich_source

        mock_api_get.side_effect = _api_payloads([
            {"source_customer_id": "BNK-001", "global_id": "G000000001"}
        ])
        _put_raw(aws_env, "bank", [{"bank_id": "BNK-001", "amount": 1000}])

        _enrich_source(aws_env, "bank", "bank_id", "2026-05-14")

        result = _get_result(aws_env, "bank")
        assert result["records"][0]["global_id"] == "G000000001"

    @patch("lifesync_identity_enricher._api_get")
    def test_unmapped_record_no_global_id(self, mock_api_get, aws_env):
        from lifesync_identity_enricher import _enrich_source

        mock_api_get.side_effect = _api_payloads([])
        _put_raw(aws_env, "bank", [{"bank_id": "BNK-999", "amount": 500}])

        _enrich_source(aws_env, "bank", "bank_id", "2026-05-14")

        result = _get_result(aws_env, "bank")
        assert "global_id" not in result["records"][0]

    @patch("lifesync_identity_enricher._api_get")
    def test_only_requested_dt_is_enriched(self, mock_api_get, aws_env):
        from lifesync_identity_enricher import _enrich_source

        mock_api_get.side_effect = _api_payloads([
            {"source_customer_id": "BNK-001", "global_id": "G000000001"}
        ])
        _put_raw(aws_env, "bank", [{"bank_id": "BNK-001"}], "2026-05-14")
        _put_raw(aws_env, "bank", [{"bank_id": "BNK-001"}], "2026-05-13")

        _enrich_source(aws_env, "bank", "bank_id", "2026-05-14")

        assert _get_result(aws_env, "bank", "2026-05-14")["records"][0]["global_id"] == "G000000001"
        assert "global_id" not in _get_result(aws_env, "bank", "2026-05-13")["records"][0]

    @patch("lifesync_identity_enricher._api_get")
    def test_hostpital_folder_alias(self, mock_api_get, aws_env):
        from lifesync_identity_enricher import _enrich_source

        mock_api_get.side_effect = _api_payloads([
            {"source_customer_id": "HSP-001", "global_id": "G000000003"}
        ])
        _put_raw(aws_env, "hostpital", [{"hospital_id": "HSP-001"}])

        _enrich_source(aws_env, "hospital", "hospital_id", "2026-05-14")

        result = _get_result(aws_env, "hostpital")
        assert result["records"][0]["global_id"] == "G000000003"


class TestSaveConsentSnapshot:
    @patch("lifesync_identity_enricher._api_get")
    def test_jsonl_saved_to_dated_key(self, mock_api_get, aws_env):
        from lifesync_identity_enricher import _save_consent_snapshot

        mock_api_get.side_effect = _api_payloads(consent_items=[
            {
                "global_id": "G000000001",
                "domain": "BANK",
                "consent_flag": "Y",
                "consent_version": "v1",
                "consent_dt": "2026-01-01",
                "revoke_dt": None,
            }
        ])

        _save_consent_snapshot(aws_env, "2026-05-14")

        obj = aws_env.get_object(Bucket=BUCKET, Key="consent/dt=2026-05-14/consent.jsonl")
        records = [json.loads(line) for line in obj["Body"].read().decode("utf-8").splitlines()]
        assert records[0]["global_id"] == "G000000001"
        assert records[0]["consent_flag"] == "Y"


class TestLambdaHandler:
    @patch("lifesync_identity_enricher._api_get")
    def test_returns_200(self, mock_api_get, aws_env):
        from lifesync_identity_enricher import lambda_handler

        mock_api_get.side_effect = _api_payloads()

        resp = lambda_handler({"date": "2026-05-14"}, {})

        assert resp["statusCode"] == 200
