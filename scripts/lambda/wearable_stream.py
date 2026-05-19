import json
import os
import logging
import base64
import boto3
from datetime import datetime, timezone, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET", "lifesync-raw")
KST = timezone(timedelta(hours=9))

# 이상치 판단 기준 (min, max)
THRESHOLDS = {
    "heart_rate":     (30, 220),   # bpm
    "steps":          (0, 50000),  # 10분 단위 최대 걸음수
    "sleep_hours":    (0, 24),
    "stress_score":   (0, 100),
    "wellness_score": (0, 100),
}

PII_FIELDS = {"name", "email", "rrn", "phone", "address"}

s3 = boto3.client("s3")


def lambda_handler(event, context):
    """
    Kinesis Event Source Mapping 트리거
    BisectBatchOnFunctionError: true → batchItemFailures 반환 필수
    """
    batch_item_failures = []
    valid_records = []

    for record in event["Records"]:
        seq = record["kinesis"]["sequenceNumber"]
        try:
            data = _decode(record["kinesis"]["data"])
            if _is_anomaly(data):
                logger.warning("[wearable] 이상치 감지 seq=%s fields=%s", seq, _anomaly_fields(data))
                continue  # 이상치는 적재하지 않고 스킵
            valid_records.append(data)
        except Exception:
            logger.exception("[wearable] 레코드 처리 실패 seq=%s", seq)
            batch_item_failures.append({"itemIdentifier": seq})

    if valid_records:
        _write_s3(valid_records)
        logger.info("[wearable] 저장 완료 count=%d", len(valid_records))

    # 처리 실패한 레코드만 Kinesis에 재처리 요청
    return {"batchItemFailures": batch_item_failures}


def _decode(encoded: str) -> dict:
    raw = base64.b64decode(encoded).decode("utf-8")
    return json.loads(raw)


def _is_anomaly(data: dict) -> bool:
    for field, (lo, hi) in THRESHOLDS.items():
        val = data.get(field)
        if val is not None and not (lo <= val <= hi):
            return True
    return False


def _anomaly_fields(data: dict) -> list:
    result = []
    for field, (lo, hi) in THRESHOLDS.items():
        val = data.get(field)
        if val is not None and not (lo <= val <= hi):
            result.append(f"{field}={val}")
    return result


def _write_s3(records: list):
    now = datetime.now(KST)
    date_str = now.strftime("%Y-%m-%d")
    ts = now.strftime("%H%M%S%f")
    key = f"wearable/{date_str}/wearable_{ts}.json"

    body = "\n".join(json.dumps(_strip_pii(r), ensure_ascii=False) for r in records)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )


def _strip_pii(record: dict) -> dict:
    return {k: v for k, v in record.items() if k not in PII_FIELDS}
