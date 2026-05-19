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


def _load_consent_data() -> tuple:
    """consent/list-all 단일 페이지네이션으로 id_map과 consent_rows를 동시에 구성.

    7번의 identity-map 스트리밍 대신 1회 API 호출로 처리하여 타임아웃 방지.
    - id_map: ls_user_id -> global_id (raw S3 enrichment용)
    - consent_rows: domain별 flatten row (consent snapshot S3 저장용)
    """
    id_map = {}
    consent_rows = []
    page, size = 0, 500
    logger.info("[identity_enricher] _load_consent_data: start")
    while True:
        logger.info("[identity_enricher] consent/list-all page=%d size=%d ...", page, size)
        batch = _api_call("/internal/consent/list-all", {"page": page, "size": size})
        for user in batch:
            ls_user_id = user.get("ls_user_id")
            global_id = user.get("global_id")
            if ls_user_id and global_id:
                id_map[str(ls_user_id)] = str(global_id)
            consents = user.get("consents", [])
            if isinstance(consents, str):
                try:
                    consents = json.loads(consents)
                except json.JSONDecodeError:
                    consents = []
            for consent in consents:
                consent_rows.append({
                    "global_id":       global_id,
                    "domain":          consent.get("domain"),
                    "consent_flag":    consent.get("consent_flag"),
                    "consent_version": consent.get("consent_version"),
                    "consent_dt":      consent.get("consent_dt"),
                    "revoke_dt":       consent.get("revoke_dt"),
                })
        logger.info("[identity_enricher] consent/list-all page=%d → %d records", page, len(batch))
        if len(batch) < size:
            break
        page += 1
    logger.info(
        "[identity_enricher] consent data loaded: %d id mappings, %d consent rows",
        len(id_map), len(consent_rows),
    )
    return id_map, consent_rows


def _enrich_source(s3, source: str, id_map: dict, date_iso: str):
    """raw JSON 파일의 ls_user_id -> global_id 매핑 후 S3 덮어쓰기."""
    prefix = f"{source}/dt={date_iso}/"
    paginator = s3.get_paginator("list_objects_v2")
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


def _write_consent_snapshot(s3, date_iso: str, consent_rows: list):
    """_load_consent_data()로 수집한 consent_rows를 NDJSON으로 S3에 저장."""
    lines = [json.dumps(row, ensure_ascii=False) for row in consent_rows]
    body = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
    key = f"consent/dt={date_iso}/consent.jsonl"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )
    logger.info("[identity_enricher] consent snapshot saved: %s (%d rows)", key, len(consent_rows))


def lambda_handler(event, context):
    """raw 계열사 JSON에 global_id 매핑 + consent 스냅샷 저장."""
    today = datetime.datetime.now(KST)
    date_iso = event.get("date") or event.get("dt") or today.strftime("%Y-%m-%d")

    s3 = _get_s3()
    logger.info("[identity_enricher] lambda_handler start: date=%s", date_iso)

    # consent/list-all 1회 호출로 id_map + consent_rows 동시 구성
    id_map, consent_rows = _load_consent_data()

    for source in SOURCES:
        logger.info("[identity_enricher] Processing source: %s", source)
        _enrich_source(s3, source, id_map, date_iso)

    _write_consent_snapshot(s3, date_iso, consent_rows)

    logger.info("[identity_enricher] global_id mapping + consent snapshot complete")
    return {"statusCode": 200, "body": "done"}


handler = lambda_handler
