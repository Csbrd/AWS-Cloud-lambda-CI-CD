import os
import logging
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal

import requests
import google.auth.transport.requests
import google.oauth2.id_token
from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "")
AWS_API_GW_URL = os.environ.get("AWS_API_GW_URL", "")

KST = timezone(timedelta(hours=9))
CHUNK_SIZE = 5_000      # API Gateway 10 MB 제한 대응
AWS_AUDIENCE = "lifesync-api"  # API GW JWT Authorizer audience


def _get_id_token() -> str:
    """Google OIDC ID 토큰 발급. API GW JWT 인증에 사용 (유효시간 1시간)."""
    auth_req = google.auth.transport.requests.Request()
    return google.oauth2.id_token.fetch_id_token(auth_req, AWS_AUDIENCE)


def today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/", methods=["POST"])
def handle_event():
    """Eventarc 호출: serving_complete/ 마커 감지 → AWS API GW 전송"""
    event = request.get_json(silent=True) or {}
    obj_name = event.get("name", "")

    if not obj_name.startswith("serving_complete/"):
        log.info("[sender] 무시: %s", obj_name)
        return jsonify({"status": "skipped"}), 200

    date_str = today_kst()
    log.info("[sender] AWS API GW 전송 시작 date=%s", date_str)
    try:
        count = _stream_to_aws(date_str)
        log.info("[sender] 전송 완료 count=%d", count)
        return jsonify({"status": "ok", "count": count}), 200
    except Exception as exc:
        log.exception("[sender] 실패")
        return jsonify({"error": str(exc)}), 500


def _serialize_row(row: dict) -> dict:
    result = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            result[k] = v.isoformat()
        elif isinstance(v, Decimal):
            result[k] = float(v)
        else:
            result[k] = v
    return result


def _stream_to_aws(date_str: str) -> int:
    """BigQuery를 CHUNK_SIZE 행씩 페이지 스트리밍하여 AWS API GW로 전달.
    전체 결과를 메모리에 올리지 않아 Cloud Run OOM을 방지한다."""
    if not AWS_API_GW_URL:
        log.warning("[sender] AWS_API_GW_URL 미설정 — 전송 생략")
        return 0

    bq = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            global_id, dynamic_score, dynamic_grade, next_best_action,
            vip_prob, signup_prob, rec_prob, health_score, update_time,
            source, ttl
        FROM `{PROJECT_ID}.lifesync_serving.customer_recommendations`
    """
    row_iter = bq.query(query).result(page_size=CHUNK_SIZE)

    # ID 토큰 한 번 발급 후 전체 배치에서 재사용 (유효시간 1시간)
    id_token = _get_id_token()
    headers = {"Authorization": f"Bearer {id_token}"}

    total_sent = 0
    for batch_index, page in enumerate(row_iter.pages):
        chunk = [_serialize_row(dict(row)) for row in page]
        if not chunk:
            continue

        payload = {
            "source": "lifesync-gcp",
            "date": date_str,
            "count": len(chunk),
            "batch_index": batch_index,
            "recommendations": chunk,
        }
        resp = requests.post(AWS_API_GW_URL, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        total_sent += len(chunk)
        log.info("[sender] batch %d 전송 완료 (누적 %d건) HTTP %d",
                 batch_index + 1, total_sent, resp.status_code)

    return total_sent


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
