import datetime
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RAW_BUCKET = "lifesync-raw"
KST = datetime.timezone(datetime.timedelta(hours=9))

SOURCES = ["bank", "card", "securities", "insurance", "online_insurance", "healthcare", "hospital"]

SOURCE_PREFIXES = {
    "hospital": ["hospital", "hostpital"],
}

_s3 = None


def _get_s3():
    global _s3
    if _s3 is None:
        config = Config(retries={'max_attempts': 5, 'mode': 'standard'})
        _s3 = boto3.client('s3', config=config)
    return _s3


def _get_private_api_config() -> dict:
    base_url = os.environ.get("PRIVATE_API_URL", "").rstrip("/")
    if not base_url:
        raise RuntimeError("Missing required environment variable: PRIVATE_API_URL")
    return {
        "base_url": base_url,
        "timeout": int(os.environ.get("PRIVATE_API_TIMEOUT", "600")),
        "api_key": os.environ.get("PRIVATE_API_KEY", ""),
    }


def _api_call(path: str, params: Optional[dict] = None) -> list:
    """페이지네이션 JSON API 호출. 응답이 list이면 그대로, dict이면 items 추출."""
    cfg = _get_private_api_config()
    query = urllib.parse.urlencode(params or {})
    url = f"{cfg['base_url']}{path}"
    if query:
        url = f"{url}?{query}"

    headers = {"Accept": "application/json"}
    if cfg["api_key"]:
        headers["x-api-key"] = cfg["api_key"]

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=cfg["timeout"]) as resp:  # nosec B310
            data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, list):
                return data
            return data.get("items", data.get("data", []))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Private API request failed: {exc.code} {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Private API request failed: {url}: {exc.reason}") from exc


def _load_id_map() -> dict:
    """ls_user_id → global_id 매핑 dict 반환.

    /internal/consent/list-all (users + consent JOIN) 페이지네이션으로 전체 조회.
    bulk identity-map 엔드포인트가 없으므로 consent 목록에서 추출.
    """
    result = {}
    page, size = 0, 10000
    while True:
        rows = _api_call("/internal/consent/list-all", {"page": page, "size": size})
        for row in rows:
            ls_user_id = row.get("ls_user_id")
            global_id = row.get("global_id")
            if ls_user_id and global_id:
                result[str(ls_user_id)] = str(global_id)
        if len(rows) < size:
            break
        page += 1
    logger.info("[identity_enricher] id_map loaded: %d entries", len(result))
    return result


def _enrich_source(s3, source: str, id_map: dict, date_iso: str):
    """raw JSON 파일에 global_id 추가 후 S3 덮어쓰기.

    raw 레코드의 ls_user_id → id_map → global_id 매핑.
    """
    prefixes = [
        f"{prefix}/dt={date_iso}/"
        for prefix in SOURCE_PREFIXES.get(source, [source])
    ]

    paginator = s3.get_paginator("list_objects_v2")
    for prefix in prefixes:
        logger.info("[identity_enricher] %s: scanning s3://%s/%s", source, RAW_BUCKET, prefix)
        for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue

                raw = json.loads(
                    s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
                )

                if isinstance(raw, dict) and "records" in raw:
                    records = raw["records"]
                    source_tag = raw.get("source", source)
                else:
                    records = raw if isinstance(raw, list) else [raw]
                    source_tag = source

                missing = 0
                for rec in records:
                    ls_user_id = str(rec.get("ls_user_id", ""))
                    global_id = id_map.get(ls_user_id)
                    if global_id:
                        rec["global_id"] = global_id
                    else:
                        missing += 1

                if missing:
                    logger.warning(
                        "[identity_enricher] %s: %d/%d records unmapped in %s",
                        source, missing, len(records), key,
                    )

                s3.put_object(
                    Bucket=RAW_BUCKET,
                    Key=key,
                    Body=json.dumps(
                        {"source": source_tag, "records": records}, ensure_ascii=False
                    ).encode("utf-8"),
                    ContentType="application/json",
                )
                logger.info(
                    "[identity_enricher] %s: overwritten %s (%d records)",
                    source, key, len(records),
                )


def _save_consent_snapshot(s3, date_iso: str):
    """consent/list-all 페이지네이션 → 도메인별 flatten → NDJSON S3 저장.

    Glue ETL이 읽는 스키마: global_id, domain, consent_flag, consent_version, consent_dt, revoke_dt
    """
    rows = []
    page, size = 0, 10000
    while True:
        batch = _api_call("/internal/consent/list-all", {"page": page, "size": size})
        for user in batch:
            global_id = user.get("global_id")
            consents = user.get("consents", [])
            if isinstance(consents, str):
                try:
                    consents = json.loads(consents)
                except json.JSONDecodeError:
                    consents = []
            for consent in consents:
                rows.append({
                    "global_id":       global_id,
                    "domain":          consent.get("domain"),
                    "consent_flag":    consent.get("consent_flag"),
                    "consent_version": consent.get("consent_version"),
                    "consent_dt":      consent.get("consent_dt"),
                    "revoke_dt":       consent.get("revoke_dt"),
                })
        if len(batch) < size:
            break
        page += 1

    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    body = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
    key = f"consent/dt={date_iso}/consent.jsonl"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )
    logger.info("[identity_enricher] consent snapshot saved: %s (%d rows)", key, len(rows))


def lambda_handler(event, context):
    """raw 계열사 JSON에 global_id 매핑 + consent 스냅샷 저장."""
    today = datetime.datetime.now(KST)
    date_iso = event.get("date") or event.get("dt") or today.strftime("%Y-%m-%d")

    s3 = _get_s3()

    # id_map은 1회만 로드 후 전체 소스에서 공유 (API 호출 최소화)
    id_map = _load_id_map()

    for source in SOURCES:
        logger.info("[identity_enricher] Processing source: %s", source)
        _enrich_source(s3, source, id_map, date_iso)

    _save_consent_snapshot(s3, date_iso)

    logger.info("[identity_enricher] global_id mapping + consent snapshot complete")
    return {"statusCode": 200, "body": "done"}


handler = lambda_handler
