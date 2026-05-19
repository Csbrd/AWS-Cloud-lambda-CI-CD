import json
import pytest
import boto3
from moto import mock_aws
from unittest.mock import MagicMock, patch

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

TABLE = "lifesync_customer_result"


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    monkeypatch.setenv("DYNAMODB_TABLE", TABLE)
    monkeypatch.setenv("REDIS_HOST", "")  # 비워두면 Redis 건너뜀


@pytest.fixture
def dynamodb_table():
    with mock_aws():
        client = boto3.resource("dynamodb", region_name="ap-northeast-2")
        table = client.create_table(
            TableName=TABLE,
            KeySchema=[
                {"AttributeName": "global_id",    "KeyType": "HASH"},
                {"AttributeName": "update_time",  "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "global_id",   "AttributeType": "S"},
                {"AttributeName": "update_time", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


def _make_event(rows: list[dict], batch_index: int = 0) -> dict:
    body = {
        "source": "lifesync-gcp",
        "date": "2026-05-08",
        "count": len(rows),
        "batch_index": batch_index,
        "recommendations": rows,
    }
    return {"body": json.dumps(body)}


def _row(
    global_id="G00000001",
    dynamic_score=92.4,
    dynamic_grade="VIP",
    next_best_action="PB_CENTER",
    vip_prob=0.94,
    signup_prob=0.81,
    rec_prob=0.77,
    health_score=88.1,
    source="GCP_lifesync",
    ttl=1752019200,  # 임의 Unix timestamp (7일 후)
):
    return {
        "global_id":        global_id,
        "dynamic_score":    dynamic_score,
        "dynamic_grade":    dynamic_grade,
        "next_best_action": next_best_action,
        "vip_prob":         vip_prob,
        "signup_prob":      signup_prob,
        "rec_prob":         rec_prob,
        "health_score":     health_score,
        "update_time":      "2026-05-08T04:30:00+09:00",
        "source":           source,
        "ttl":              ttl,
    }


class TestRecommendationEngine:
    def test_dynamodb_write(self, dynamodb_table):
        import recommendation_engine as eng
        eng.dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")

        with patch.object(eng, "_query_aurora_products", return_value={}):
            event = _make_event([_row("G00000001"), _row("G00000002", dynamic_score=65.0, dynamic_grade="GOLD")])
            resp = eng.handler(event, {})

        assert resp["statusCode"] == 200

        items = dynamodb_table.scan()["Items"]
        global_ids = [i["global_id"] for i in items]
        assert "G00000001" in global_ids
        assert "G00000002" in global_ids

    def test_dynamodb_fields_stored(self, dynamodb_table):
        """DynamoDB에 serving view 필드가 정확히 저장되는지 확인"""
        import recommendation_engine as eng
        eng.dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")

        with patch.object(eng, "_query_aurora_products", return_value={}):
            event = _make_event([_row("G00000001", ttl=1752019200)])
            eng.handler(event, {})

        item = dynamodb_table.scan()["Items"][0]
        assert item["dynamic_grade"] == "VIP"
        assert item["next_best_action"] == "PB_CENTER"
        assert float(item["vip_prob"]) == pytest.approx(0.94, abs=0.001)
        assert float(item["health_score"]) == pytest.approx(88.1, abs=0.01)
        assert item["source"] == "GCP_lifesync"
        assert int(item["ttl"]) == 1752019200  # GCP에서 전달된 ttl 그대로 저장

    def test_empty_recommendations(self, dynamodb_table):
        import recommendation_engine as eng
        eng.dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")

        event = _make_event([])
        resp = eng.handler(event, {})
        assert resp["statusCode"] == 200
        assert json.loads(resp["body"])["stored"] == 0

    def test_care_grade_stored(self, dynamodb_table):
        """CARE 등급 고객도 DynamoDB에 정상 저장"""
        import recommendation_engine as eng
        eng.dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")

        row = _row("G00000003", dynamic_score=45.0, dynamic_grade="CARE",
                   next_best_action="RETENTION", vip_prob=0.12)
        event = _make_event([row])
        resp = eng.handler(event, {})
        assert resp["statusCode"] == 200

        item = dynamodb_table.scan()["Items"][0]
        assert item["dynamic_grade"] == "CARE"
        assert item["next_best_action"] == "RETENTION"

    def test_redis_caches_vip_gold_only(self, dynamodb_table):
        """VIP/GOLD만 Redis 캐시, CARE는 건너뜀"""
        import recommendation_engine as eng
        eng.dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")

        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_redis.pipeline.return_value = mock_pipe
        eng._redis_conn = mock_redis

        rows = [
            _row("G00000001", dynamic_grade="VIP"),
            _row("G00000002", dynamic_score=65.0, dynamic_grade="GOLD"),
            _row("G00000003", dynamic_score=40.0, dynamic_grade="CARE",
                 next_best_action="RETENTION", vip_prob=0.1),
        ]
        with patch.dict(os.environ, {"REDIS_HOST": "localhost"}):
            eng.REDIS_HOST = "localhost"
            with patch.object(eng, "_query_aurora_products", return_value={}):
                event = _make_event(rows)
                eng.handler(event, {})

        # setex 호출 횟수: VIP + GOLD = 2회 (CARE 제외)
        assert mock_pipe.setex.call_count == 2

        eng._redis_conn = None
        eng.REDIS_HOST = ""

    def test_aurora_product_included_in_redis(self, dynamodb_table):
        """Aurora 상품 조회 결과가 Redis 캐시 payload에 포함되는지 확인"""
        import recommendation_engine as eng
        eng.dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")

        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_redis.pipeline.return_value = mock_pipe
        eng._redis_conn = mock_redis

        aurora_products = [{"product_id": "P001", "product_name": "ETF상품", "category_name": "투자"}]

        rows = [_row("G00000001", dynamic_grade="VIP", next_best_action="ETF_PRODUCT")]
        with patch.dict(os.environ, {"REDIS_HOST": "localhost"}):
            eng.REDIS_HOST = "localhost"
            with patch.object(eng, "_query_aurora_products", return_value={"ETF_PRODUCT": aurora_products}):
                event = _make_event(rows)
                eng.handler(event, {})

        call_args = mock_pipe.setex.call_args_list[0]
        cached_payload = json.loads(call_args[0][2])
        assert cached_payload["recommended_products"] == aurora_products

        eng._redis_conn = None
        eng.REDIS_HOST = ""
