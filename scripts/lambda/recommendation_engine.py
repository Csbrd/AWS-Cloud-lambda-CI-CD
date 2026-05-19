"""
lifesync recommendation_engine.py
GCP sender.py → API Gateway → Lambda → DynamoDB / Aurora / ElastiCache Redis

역할:
  - DynamoDB: 고객 최신 상태 저장 (global_id, dynamic_score, dynamic_grade, next_best_action, 확률값)
  - Aurora  : next_best_action 기반 추천 상품 조회 (recommend_rule / product_master / category_master)
  - Redis   : VIP/GOLD 고객 대시보드 결과 + 추천 상품 캐시 (10분 TTL)

환경변수:
  DYNAMODB_TABLE        lifesync_customer_result
  REDIS_HOST            lifesync-dev-redis.vqzsjn.0001.apn2.cache.amazonaws.com
  REDIS_PORT            6379
  AURORA_SECRET_NAME    lifesync/aurora/credentials
  AURORA_DB             lifesync
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
import psycopg2.extras
import redis

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

DYNAMODB_TABLE     = os.environ.get("DYNAMODB_TABLE", "lifesync_customer_result")
REDIS_HOST         = os.environ.get("REDIS_HOST", "")
REDIS_PORT         = int(os.environ.get("REDIS_PORT", "6379"))
AURORA_SECRET_NAME = os.environ.get("AURORA_SECRET_NAME", "lifesync/aurora/credentials")
AURORA_DB          = os.environ.get("AURORA_DB", "lifesync")
REDIS_CACHE_TTL    = 600  # 10분

KST = timezone(timedelta(hours=9))

dynamodb      = boto3.resource("dynamodb")
secretsmanager = boto3.client("secretsmanager")

_redis_conn  = None
_aurora_conn = None


def _get_redis_conn():
    global _redis_conn
    if _redis_conn:
        return _redis_conn
    _redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    return _redis_conn


def _get_aurora_conn():
    global _aurora_conn
    if _aurora_conn and not _aurora_conn.closed:
        return _aurora_conn
    secret = json.loads(
        secretsmanager.get_secret_value(SecretId=AURORA_SECRET_NAME)["SecretString"]
    )
    _aurora_conn = psycopg2.connect(
        host=secret["host"],
        port=secret.get("port", 5432),
        dbname=AURORA_DB,
        user=secret["username"],
        password=secret["password"],
    )
    return _aurora_conn


def handler(event, context):
    try:
        body = event.get("body", "{}")
        if isinstance(body, str):
            body = json.loads(body)

        date_str    = body.get("date", "")
        rows        = body.get("recommendations", [])
        batch_index = body.get("batch_index", 0)

        log.info("batch_index=%d rows=%d date=%s", batch_index, len(rows), date_str)

        if not rows:
            return _response(200, {"status": "ok", "stored": 0})

        update_time = datetime.now(KST).isoformat()

        _write_dynamodb(rows, update_time)

        vip_gold_rows = [r for r in rows if r.get("dynamic_grade") in ("VIP", "GOLD")]
        if vip_gold_rows:
            action_types = {r.get("next_best_action", "") for r in vip_gold_rows}
            product_map  = _query_aurora_products(action_types)
            if REDIS_HOST:
                _write_redis(vip_gold_rows, product_map, update_time)

        return _response(200, {"status": "ok", "stored": len(rows)})

    except Exception:
        log.exception("handler 실패")
        return _response(500, {"error": "internal error"})


def _write_dynamodb(rows: list[dict], update_time: str):
    table = dynamodb.Table(DYNAMODB_TABLE)

    chunk_size = 25  # DynamoDB BatchWriteItem 최대 25건
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        with table.batch_writer() as batch:
            for row in chunk:
                # ttl은 GCP에서 계산된 Unix timestamp(7일) 우선 사용
                ttl = row.get("ttl") or int(
                    (datetime.now(KST) + timedelta(days=7)).timestamp()
                )
                batch.put_item(Item={
                    "global_id":        row.get("global_id", ""),
                    "update_time":      update_time,
                    "dynamic_score":    str(round(float(row.get("dynamic_score", 0)), 1)),
                    "dynamic_grade":    row.get("dynamic_grade", "CARE"),
                    "next_best_action": row.get("next_best_action", "RETENTION"),
                    "vip_prob":         str(round(float(row.get("vip_prob", 0)), 4)),
                    "signup_prob":      str(round(float(row.get("signup_prob", 0)), 4)),
                    "rec_prob":         str(round(float(row.get("rec_prob", 0)), 4)),
                    "health_score":     str(round(float(row.get("health_score", 0)), 1)),
                    "source":           row.get("source", "GCP_lifesync"),
                    "ttl":              ttl,
                })

    log.info("[DynamoDB] %d건 완료", len(rows))


def _query_aurora_products(action_types: set[str]) -> dict[str, list[dict]]:
    """next_best_action 타입별 추천 상품 Top 3 조회.

    recommend_rule.priority 순으로 정렬 후 action_type당 최대 3개 반환.
    Returns: {action_type: [{product_id, product_name, category_name}, ...]}
    """
    if not action_types:
        return {}

    try:
        conn = _get_aurora_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    rr.action_type,
                    pm.product_id,
                    pm.product_name,
                    cm.category_name
                FROM recommend_rule rr
                JOIN product_master  pm ON rr.product_id  = pm.product_id
                JOIN category_master cm ON pm.category_id = cm.category_id
                WHERE rr.action_type = ANY(%s)
                ORDER BY rr.action_type, rr.priority
                """,
                (list(action_types),),
            )
            rows = cur.fetchall()
    except Exception:
        log.exception("[Aurora] 상품 조회 실패")
        global _aurora_conn
        _aurora_conn = None  # 다음 호출 시 재연결
        return {}

    product_map: dict[str, list[dict]] = {}
    for row in rows:
        action = row["action_type"]
        if action not in product_map:
            product_map[action] = []
        if len(product_map[action]) < 3:
            product_map[action].append({
                "product_id":    row["product_id"],
                "product_name":  row["product_name"],
                "category_name": row["category_name"],
            })

    log.info("[Aurora] action_types=%s 조회 완료", action_types)
    return product_map


def _write_redis(rows: list[dict], product_map: dict[str, list[dict]], update_time: str):
    """VIP/GOLD 고객 대시보드 결과 + 추천 상품을 개별 키로 캐시 (10분 TTL)."""
    r = _get_redis_conn()
    pipe = r.pipeline()
    for row in rows:
        action  = row.get("next_best_action", "")
        payload = {
            **row,
            "update_time":          update_time,
            "recommended_products": product_map.get(action, []),
        }
        key = f"lifesync:dashboard:{row['global_id']}"
        pipe.setex(key, REDIS_CACHE_TTL, json.dumps(payload))
    pipe.execute()
    log.info("[Redis] %d건 캐시 완료 (VIP/GOLD)", len(rows))


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
