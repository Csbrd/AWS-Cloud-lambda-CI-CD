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

# 페이지네이션 요청당 최대 대기 시간 (PRIVATE_API_TIMEOUT 무관하게 강제 적용)
_PER_REQUEST_TIMEOUT = 30
_S3_MULTIPART_MIN_PART = 5 * 1024 * 1024  # S3 multipart 최소 파트 크기 5MB

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


def _api_call_raw(path: str, params: Optional[dict] = None) -> dict:
    """API 호출 후 전체 응답 dict 반환. 요청당 30초 timeout, 최대 3회 재시도."""
    cfg = _get_private_api_config()
    timeout = min(cfg["timeout"], _PER_REQUEST_TIMEOUT)
    query = urllib.parse.urlencode(params or {})
    url = f"{cfg['base_url']}{path}"
    if query:
        url = f"{url}?{query}"

    headers = {"Accept": "application/json"}
    if cfg["api_key"]:
        headers["x-api-key"] = cfg["api_key"]

    req = urllib.request.Request(url, headers=headers, method="GET")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"Private API request failed: {exc.code} {url}") from exc
        except urllib.error.URLError as exc:
            if attempt < 2:
                logger.warning(
                    "[identity_enricher] request timeout (attempt %d/3), retrying: %s",
                    attempt + 1, url,
                )
            else:
                raise RuntimeError(f"Private API request failed: {url}: {exc.reason}") from exc
    raise RuntimeError("unreachable")


def _load_consent_data(s3, date_iso: str) -> dict:
    """consent/list-all 커서 페이지네이션으로 id_map 구성 + consent snapshot S3 스트리밍 저장.

    consent/consent.jsonl이 이미 존재하면 API 호출을 스킵하고 기존 파일에서 id_map 복원.

    Returns:
        id_map: {ls_user_id: global_id}
    """
    key = "consent/consent.jsonl"

    # 이미 스냅샷이 있으면 기존 파일에서 id_map 복원 후 스킵
    # consent.jsonl에 ls_user_id가 포함되어 있어 id_map 재구성 가능
    try:
        s3.head_object(Bucket=RAW_BUCKET, Key=key)
        logger.info("[identity_enricher] consent snapshot already exists — skipping API call, restoring id_map from s3://%s/%s", RAW_BUCKET, key)
        id_map = {}
        body = s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
                ls_uid = rec.get("ls_user_id")
                gid = rec.get("global_id")
                if ls_uid and gid:
                    id_map[str(ls_uid)] = str(gid)
            except json.JSONDecodeError:
                continue
        logger.info("[identity_enricher] id_map restored: %d entries", len(id_map))
        return id_map
    except s3.exceptions.ClientError:
        pass  # 파일 없음 → 신규 생성

    id_map = {}
    size = 10000
    after = None
    total_rows = 0
    mp = s3.create_multipart_upload(Bucket=RAW_BUCKET, Key=key, ContentType="application/x-ndjson")
    upload_id = mp["UploadId"]
    parts = []
    part_num = 1
    buf: list = []
    buf_size = 0

    logger.info("[identity_enricher] _load_consent_data: start, streaming → %s", key)
    try:
        while True:
            params: dict = {"size": size}
            if after:
                params["after"] = after
            logger.info("[identity_enricher] consent/list-all after=%s size=%d ...", after, size)
            resp = _api_call_raw("/internal/consent/list-all", params)
            batch = resp.get("items", [])
            logger.info("[identity_enricher] consent/list-all after=%s → %d records", after, len(batch))

            for user in batch:
                ls_user_id = user.get("ls_user_id")
                global_id = user.get("global_id")
                if ls_user_id and global_id:
                    id_map[str(ls_user_id)] = str(global_id)
                consents = user.get("consents") or []
                if isinstance(consents, str):
                    try:
                        consents = json.loads(consents)
                    except json.JSONDecodeError:
                        consents = []
                for consent in consents:
                    line = json.dumps({
                        "global_id":       global_id,
                        "ls_user_id":      ls_user_id,
                        "domain":          consent.get("domain"),
                        "consent_flag":    consent.get("consent_flag"),
                        "consent_version": consent.get("consent_version"),
                        "consent_dt":      consent.get("consent_dt"),
                        "revoke_dt":       consent.get("revoke_dt"),
                    }, ensure_ascii=False) + "\n"
                    encoded = line.encode("utf-8")
                    buf.append(encoded)
                    buf_size += len(encoded)
                    total_rows += 1

            if buf_size >= _S3_MULTIPART_MIN_PART:
                body = b"".join(buf)
                part = s3.upload_part(
                    Bucket=RAW_BUCKET, Key=key,
                    PartNumber=part_num, UploadId=upload_id, Body=body,
                )
                parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
                logger.info(
                    "[identity_enricher] multipart part %d uploaded (%d rows so far)",
                    part_num, total_rows,
                )
                part_num += 1
                buf = []
                buf_size = 0

            after = resp.get("next_after")
            if not after or len(batch) < size:
                break

        # 남은 버퍼를 마지막 파트로 업로드 (S3 마지막 파트는 크기 제한 없음)
        if buf:
            body = b"".join(buf)
            part = s3.upload_part(
                Bucket=RAW_BUCKET, Key=key,
                PartNumber=part_num, UploadId=upload_id, Body=body,
            )
            parts.append({"PartNumber": part_num, "ETag": part["ETag"]})

        if parts:
            s3.complete_multipart_upload(
                Bucket=RAW_BUCKET, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            logger.info(
                "[identity_enricher] consent snapshot saved: %s (%d rows, %d parts)",
                key, total_rows, len(parts),
            )
        else:
            # 데이터 없음 — multipart abort 후 빈 파일 저장
            s3.abort_multipart_upload(Bucket=RAW_BUCKET, Key=key, UploadId=upload_id)
            s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=b"", ContentType="application/x-ndjson")
            logger.info("[identity_enricher] consent snapshot saved (empty): %s", key)

    except Exception:
        try:
            s3.abort_multipart_upload(Bucket=RAW_BUCKET, Key=key, UploadId=upload_id)
        except Exception:
            pass
        raise

    logger.info("[identity_enricher] id_map loaded: %d entries", len(id_map))
    return id_map


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


def lambda_handler(event, context):
    """raw 계열사 JSON에 global_id 매핑 + consent 스냅샷 저장."""
    today = datetime.datetime.now(KST)
    date_iso = event.get("date") or event.get("dt") or today.strftime("%Y-%m-%d")

    s3 = _get_s3()
    logger.info("[identity_enricher] lambda_handler start: date=%s", date_iso)

    # consent/list-all 커서 페이지네이션: id_map 구성 + consent snapshot S3 스트리밍
    id_map = _load_consent_data(s3, date_iso)

    for source in SOURCES:
        logger.info("[identity_enricher] Processing source: %s", source)
        _enrich_source(s3, source, id_map, date_iso)

    logger.info("[identity_enricher] global_id mapping + consent snapshot complete")
    return {"statusCode": 200, "body": "done"}


handler = lambda_handler
