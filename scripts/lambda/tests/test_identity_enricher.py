"""
lifesync-identity-enricher Lambda tests.

Install:
  pip install pytest boto3 "moto[s3]"
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

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
    monkeypatch.setenv("PRIVATE_API_URL", "http://private-api.local")


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


def _mock_urlopen(payload):
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(payload).encode("utf-8")
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


class TestPrivateApiHelpers:
    def test_private_api_url_required(self, monkeypatch):
        from lifesync_identity_enricher import _get_private_api_config

        monkeypatch.delenv("PRIVATE_API_URL", raising=False)
        with pytest.raises(RuntimeError, match="PRIVATE_API_URL"):
            _get_private_api_config()

    def test_api_call_returns_list_directly(self):
        from lifesync_identity_enricher import _api_call

        with patch("urllib.request.urlopen", return_value=_mock_urlopen([{"a": 1}])):
            result = _api_call("/test")
        assert result == [{"a": 1}]

    def test_api_call_extracts_items_from_dict(self):
        from lifesync_identity_enricher import _api_call

        with patch("urllib.request.urlopen", return_value=_mock_urlopen({"items": [{"b": 2}]})):
            result = _api_call("/test")
        assert result == [{"b": 2}]

    def test_api_call_raw_retries_on_timeout(self):
        from lifesync_identity_enricher import _api_call_raw
        import urllib.error

        good_resp = _mock_urlopen({"items": [], "next_after": None})
        side_effects = [
            urllib.error.URLError("timed out"),
            good_resp,
        ]
        with patch("urllib.request.urlopen", side_effect=side_effects):
            result = _api_call_raw("/test")
        assert result == {"items": [], "next_after": None}


class TestLoadConsentData:
    @patch("lifesync_identity_enricher._api_call_raw")
    def test_builds_id_map_and_writes_consent_file(self, mock_api_call_raw, aws_env):
        from lifesync_identity_enricher import _load_consent_data

        mock_api_call_raw.return_value = {
            "items": [
                {
                    "global_id": "G000000001",
                    "ls_user_id": "LS000000001",
                    "consents": [
                        {"domain": "BANK", "consent_flag": "Y", "consent_version": "v1",
                         "consent_dt": "2026-01-01", "revoke_dt": None},
                    ],
                }
            ],
            "next_after": None,
        }

        id_map = _load_consent_data(aws_env, "2026-05-14")

        assert id_map == {"LS000000001": "G000000001"}

        obj = aws_env.get_object(Bucket=BUCKET, Key="consent/consent.jsonl")
        records = [json.loads(line) for line in obj["Body"].read().decode("utf-8").splitlines()]
        assert records[0]["global_id"] == "G000000001"
        assert records[0]["ls_user_id"] == "LS000000001"
        assert records[0]["domain"] == "BANK"

    @patch("lifesync_identity_enricher._api_call_raw")
    def test_empty_response_writes_empty_file(self, mock_api_call_raw, aws_env):
        from lifesync_identity_enricher import _load_consent_data

        mock_api_call_raw.return_value = {"items": [], "next_after": None}

        id_map = _load_consent_data(aws_env, "2026-05-14")

        assert id_map == {}
        obj = aws_env.get_object(Bucket=BUCKET, Key="consent/consent.jsonl")
        assert obj["Body"].read() == b""


class TestEnrichSource:
    def test_global_id_added(self, aws_env):
        from lifesync_identity_enricher import _enrich_source

        id_map = {"LS000000001": "G000000001"}
        _put_raw(aws_env, "bank", [{"ls_user_id": "LS000000001", "amount": 1000}])

        _enrich_source(aws_env, "bank", id_map, "2026-05-14")

        result = _get_result(aws_env, "bank")
        assert result["records"][0]["global_id"] == "G000000001"

    def test_unmapped_record_no_global_id(self, aws_env):
        from lifesync_identity_enricher import _enrich_source

        id_map = {}
        _put_raw(aws_env, "bank", [{"ls_user_id": "LS999999999", "amount": 500}])

        _enrich_source(aws_env, "bank", id_map, "2026-05-14")

        result = _get_result(aws_env, "bank")
        assert "global_id" not in result["records"][0]

    def test_only_requested_dt_is_enriched(self, aws_env):
        from lifesync_identity_enricher import _enrich_source

        id_map = {"LS000000001": "G000000001"}
        _put_raw(aws_env, "bank", [{"ls_user_id": "LS000000001"}], "2026-05-14")
        _put_raw(aws_env, "bank", [{"ls_user_id": "LS000000001"}], "2026-05-13")

        _enrich_source(aws_env, "bank", id_map, "2026-05-14")

        assert _get_result(aws_env, "bank", "2026-05-14")["records"][0]["global_id"] == "G000000001"
        assert "global_id" not in _get_result(aws_env, "bank", "2026-05-13")["records"][0]


class TestLambdaHandler:
    @patch("lifesync_identity_enricher._load_consent_data")
    def test_returns_200(self, mock_load, aws_env):
        from lifesync_identity_enricher import lambda_handler

        mock_load.return_value = {}

        resp = lambda_handler({"date": "2026-05-14"}, {})

        assert resp["statusCode"] == 200
