import base64
import json
import pytest
import boto3
from moto import mock_aws

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BUCKET = "lifesync-raw"


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    monkeypatch.setenv("S3_BUCKET", BUCKET)


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="ap-northeast-2")
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-2"},
        )
        yield client


def _kinesis_event(records: list[dict]) -> dict:
    return {
        "Records": [
            {
                "kinesis": {
                    "sequenceNumber": f"seq-{i}",
                    "data": base64.b64encode(
                        json.dumps(r).encode()
                    ).decode(),
                }
            }
            for i, r in enumerate(records)
        ]
    }


def _normal_record():
    return {
        "global_id": "G00000001",
        "heart_rate": 75,
        "steps": 3000,
        "sleep_hours": 7,
        "stress_score": 40,
        "wellness_score": 80,
    }


class TestWearableStream:
    def test_normal_record_stored(self, s3):
        from wearable_stream import lambda_handler
        event = _kinesis_event([_normal_record()])
        result = lambda_handler(event, {})
        assert result["batchItemFailures"] == []

    def test_anomaly_skipped(self, s3):
        from wearable_stream import lambda_handler
        anomaly = {**_normal_record(), "heart_rate": 300}
        event = _kinesis_event([anomaly])
        result = lambda_handler(event, {})
        assert result["batchItemFailures"] == []
        # 이상치는 S3에 저장되지 않아야 함
        objects = s3.list_objects_v2(Bucket=BUCKET, Prefix="wearable/")
        assert objects.get("KeyCount", 0) == 0

    def test_pii_stripped(self, s3):
        from wearable_stream import lambda_handler
        record = {**_normal_record(), "name": "홍길동", "phone": "010-1234-5678"}
        event = _kinesis_event([record])
        lambda_handler(event, {})

        objects = s3.list_objects_v2(Bucket=BUCKET, Prefix="wearable/")
        key = objects["Contents"][0]["Key"]
        stored = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        assert "name" not in stored
        assert "phone" not in stored

    def test_invalid_json_returns_failure(self, s3):
        from wearable_stream import lambda_handler
        event = {
            "Records": [
                {"kinesis": {"sequenceNumber": "seq-0", "data": "not-base64!!!"}}
            ]
        }
        result = lambda_handler(event, {})
        assert len(result["batchItemFailures"]) == 1
