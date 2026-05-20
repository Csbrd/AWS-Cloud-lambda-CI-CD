import os
import logging
from datetime import datetime, timezone, timedelta

from flask import Flask, request, jsonify
from google.cloud import bigquery, storage

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "")
REGION     = os.environ.get("REGION", "asia-northeast3")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "lifesync-data-lake")

VIP_MODEL_RESOURCE_NAME    = os.environ.get("VIP_MODEL_RESOURCE_NAME", "")
SIGNUP_MODEL_RESOURCE_NAME = os.environ.get("SIGNUP_MODEL_RESOURCE_NAME", "")
REC_MODEL_RESOURCE_NAME    = os.environ.get("REC_MODEL_RESOURCE_NAME", "")
HEALTH_MODEL_RESOURCE_NAME = os.environ.get("HEALTH_MODEL_RESOURCE_NAME", "")

KST = timezone(timedelta(hours=9))


def today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _any_model() -> bool:
    return any([VIP_MODEL_RESOURCE_NAME, SIGNUP_MODEL_RESOURCE_NAME,
                REC_MODEL_RESOURCE_NAME, HEALTH_MODEL_RESOURCE_NAME])


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/load-bq", methods=["POST"])
def load_bq():
    """Cloud Scheduler 03:20 KST 호출: GCS Parquet → BigQuery 적재 (Storage Transfer 완료 후)"""
    date_str = today_kst()
    log.info("[/load-bq] date=%s", date_str)
    try:
        _load_gcs_to_bq(date_str)
        return jsonify({"status": "ok", "date": date_str}), 200
    except Exception as exc:
        log.exception("[/load-bq] 실패")
        return jsonify({"error": str(exc)}), 500


@app.route("/run", methods=["POST"])
def run():
    """Cloud Scheduler 호출: Vertex AI Batch Prediction 4개 job 제출"""
    date_str = today_kst()
    log.info("[/run] date=%s", date_str)
    try:
        if _any_model():
            _run_batch_prediction(date_str)
        else:
            log.warning("[/run] 모델 미설정 — 더미 예측 결과 생성")
            _write_mock_predictions()
        return jsonify({"status": "ok", "date": date_str}), 200
    except Exception as exc:
        log.exception("[/run] 실패")
        return jsonify({"error": str(exc)}), 500


@app.route("/dynamic-score", methods=["POST"])
def dynamic_score():
    """Cloud Scheduler 04:30 KST 호출: 예측 결과 변환 → 서빙 레이어 갱신"""
    date_str = today_kst()
    log.info("[/dynamic-score] 서빙 레이어 갱신 date=%s", date_str)
    try:
        if _any_model():
            _convert_prediction_results()
        _refresh_serving_table()
        _write_gcs_marker(f"serving_complete/{date_str}.done")
        return jsonify({"status": "ok", "date": date_str}), 200
    except Exception as exc:
        log.exception("[/dynamic-score] 실패")
        return jsonify({"error": str(exc)}), 500


NUMERIC_FEATURES = [
    # 금융
    "balance_30d_avg", "asset_growth_90d", "card_spend_30d",
    "invest_total", "invest_ratio", "etf_ratio", "policy_cnt",
    # 건강
    "avg_steps_30d", "avg_hr_30d", "stress_avg_30d",
    "hospital_visit_90d", "health_risk_score", "step_growth_30d",
    # 행동
    "login_cnt_30d", "avg_session_min", "push_click_rate",
    "recommend_click_rate", "last_active_days",
    # 관계
    "affiliate_cnt", "consent_ratio", "membership_days", "cross_product_score",
    # 성장
    "spend_growth_90d", "invest_growth_90d", "wellness_growth_30d",
    # Risk
    "inactive_days", "card_drop_ratio", "asset_drop_ratio", "complaint_flag",
]

# health 모델 훈련 피처 (health_risk_score 제외 — target과 동일하므로)
# 컬럼 순서가 train.py의 health_features 와 반드시 일치해야 함
HEALTH_FEATURES = [f for f in NUMERIC_FEATURES if f != "health_risk_score"]


# GCS에서 BigQuery로 적재할 테이블 목록 (table_name, dataset_id)
_GCS_TO_BQ_TABLES = [
    ("customer_360_profile", "lifesync_curated"),
    ("ai_feature_table",     "lifesync_curated"),
    ("score_mart",           "lifesync_curated"),
    ("health_mart",          "lifesync_curated"),
    ("vip_mart",             "lifesync_curated"),
    ("recommendation_mart",  "lifesync_curated"),
]


def _load_gcs_to_bq(date_str: str):
    """GCS Parquet (dt=YYYY-MM-DD 파티션) → BigQuery WRITE_TRUNCATE 적재.

    CLAUDE.md: Dataflow 사용 금지 — BigQuery Load Job으로 직접 적재.
    Storage Transfer가 S3 Curated 전체를 GCS에 복사하므로
    날짜 파티션 경로를 지정해 해당일 데이터만 덮어씀.
    """
    bq = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    for table_name, dataset_id in _GCS_TO_BQ_TABLES:
        gcs_uri = f"gs://{GCS_BUCKET}/curated/{table_name}/dt={date_str}/*.parquet"
        table_ref = f"{PROJECT_ID}.{dataset_id}.{table_name}"
        job = bq.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        job.result()
        log.info("[_load_gcs_to_bq] %s → %s 완료", gcs_uri, table_ref)


def _ensure_feature_table():
    """XGBoost 배치 예측 입력용 피처 테이블 생성.

    모델은 익명(feature_names=None)으로 저장되므로 컬럼 순서가 훈련 순서와 일치해야 함.
    - ai_feature_pred      : row_id + 27 피처 = 28 컬럼 (VIP / Signup / Rec 모델용)
    - ai_feature_pred_health: row_id + 26 피처 = 27 컬럼, health_risk_score 제외 (Health 모델용)
    row_id = ROW_NUMBER() OVER (ORDER BY global_id) — 예측 후 global_id 복구에 사용
    """
    bq = bigquery.Client(project=PROJECT_ID)

    cols_28 = ", ".join(
        [f"COALESCE(CAST({c} AS FLOAT64), 0.0) AS {c}" for c in NUMERIC_FEATURES]
    )
    bq.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_curated.ai_feature_pred` AS
        SELECT ROW_NUMBER() OVER (ORDER BY global_id) AS row_id, {cols_28}
        FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
    """).result()

    cols_27 = ", ".join(
        [f"COALESCE(CAST({c} AS FLOAT64), 0.0) AS {c}" for c in HEALTH_FEATURES]
    )
    bq.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_curated.ai_feature_pred_health` AS
        SELECT ROW_NUMBER() OVER (ORDER BY global_id) AS row_id, {cols_27}
        FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
    """).result()

    log.info("[_ensure_feature_table] 완료 (28 + 27 피처 — row_id 포함)")


def _run_batch_prediction(date_str: str):
    """4개 모델 배치 예측 job 비동기 제출."""
    _ensure_feature_table()
    from google.cloud import aiplatform
    aiplatform.init(project=PROJECT_ID, location=REGION)

    # health 모델은 27-피처 전용 테이블 사용 (훈련 컬럼 순서 일치)
    model_sources = {
        "vip":    (VIP_MODEL_RESOURCE_NAME,    f"bq://{PROJECT_ID}.lifesync_curated.ai_feature_pred"),
        "signup": (SIGNUP_MODEL_RESOURCE_NAME, f"bq://{PROJECT_ID}.lifesync_curated.ai_feature_pred"),
        "rec":    (REC_MODEL_RESOURCE_NAME,    f"bq://{PROJECT_ID}.lifesync_curated.ai_feature_pred"),
        "health": (HEALTH_MODEL_RESOURCE_NAME, f"bq://{PROJECT_ID}.lifesync_curated.ai_feature_pred_health"),
    }
    for prefix, (model_name, bq_source) in model_sources.items():
        if not model_name:
            log.info("[_run_batch_prediction] %s 모델 미설정 — 건너뜀", prefix)
            continue
        aiplatform.BatchPredictionJob.create(
            job_display_name=f"lifesync-{prefix}-{date_str}",
            model_name=model_name,
            instances_format="bigquery",
            predictions_format="bigquery",
            bigquery_source=bq_source,
            bigquery_destination_prefix=f"bq://{PROJECT_ID}.lifesync_ml.{prefix}_raw_pred",
            machine_type="n1-standard-4",
            starting_replica_count=1,
            max_replica_count=2,
            sync=False,
        )
        log.info("[_run_batch_prediction] %s job 제출 완료", prefix)


def _find_latest_pred_table(bq: bigquery.Client, prefix: str) -> str | None:
    """INFORMATION_SCHEMA에서 prefix 로 시작하는 최신 테이블명 반환. 없으면 None."""
    rows = list(bq.query(f"""
        SELECT table_name
        FROM `{PROJECT_ID}.lifesync_ml.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE '{prefix}%'
        ORDER BY creation_time DESC
        LIMIT 1
    """).result())
    return rows[0].table_name if rows else None


def _convert_prediction_results():
    """Vertex AI raw 예측 테이블 → 4개 설계 문서 기준 결과 테이블 변환.

    ai_feature_pred 에 global_id 가 포함되어 있으므로
    Vertex AI 출력 테이블에도 global_id 가 passthrough됨 → 직접 사용.
    모든 결과 테이블은 global_id ORDER 로 정렬하여 적재.
    """
    bq = bigquery.Client(project=PROJECT_ID)

    # ── VIP (Classifier: prediction = [p_neg, p_pos]) ─────────────────────────
    if VIP_MODEL_RESOURCE_NAME:
        tbl = _find_latest_pred_table(bq, "vip_raw_pred")
        if tbl is None:
            log.info("[_convert] vip_raw_pred 없음 — 기존 vip_prediction_result 유지")
        else:
            bq.query(f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.vip_prediction_result` AS
                SELECT
                    feat.global_id,
                    CURRENT_DATE()  AS feature_dt,
                    pred.balance_30d_avg,
                    pred.invest_total,
                    CAST(pred.affiliate_cnt AS INT64) AS affiliate_cnt,
                    IF(p >= 0.5, 1, 0)           AS predicted_label,
                    [p, ROUND(1.0 - p, 4)]       AS predicted_scores,
                    CURRENT_TIMESTAMP()          AS prediction_time
                FROM (
                    SELECT *,
                        COALESCE(
                            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(prediction), '$[1]') AS FLOAT64),
                            SAFE_CAST(prediction AS FLOAT64), 0.0
                        ) AS p
                    FROM `{PROJECT_ID}.lifesync_ml.{tbl}`
                ) pred
                JOIN (
                    SELECT ROW_NUMBER() OVER (ORDER BY global_id) AS row_id, global_id
                    FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
                ) feat ON pred.row_id = feat.row_id
                ORDER BY feat.global_id
            """).result()
            log.info("[_convert] vip_prediction_result 완료")

    # ── Signup (Classifier) ────────────────────────────────────────────────────
    if SIGNUP_MODEL_RESOURCE_NAME:
        tbl = _find_latest_pred_table(bq, "signup_raw_pred")
        if tbl is None:
            log.info("[_convert] signup_raw_pred 없음 — 기존 signup_prediction_result 유지")
        else:
            bq.query(f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.signup_prediction_result` AS
                SELECT
                    feat.global_id,
                    CURRENT_DATE()  AS feature_dt,
                    pred.balance_30d_avg,
                    pred.card_spend_30d,
                    pred.invest_ratio,
                    CAST(pred.login_cnt_30d       AS INT64) AS login_cnt_30d,
                    pred.recommend_click_rate,
                    CAST(pred.affiliate_cnt       AS INT64) AS affiliate_cnt,
                    IF(p >= 0.5, 1, 0)           AS predicted_label,
                    [p, ROUND(1.0 - p, 4)]       AS predicted_scores,
                    CURRENT_TIMESTAMP()          AS prediction_time
                FROM (
                    SELECT *,
                        COALESCE(
                            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(prediction), '$[1]') AS FLOAT64),
                            SAFE_CAST(prediction AS FLOAT64), 0.0
                        ) AS p
                    FROM `{PROJECT_ID}.lifesync_ml.{tbl}`
                ) pred
                JOIN (
                    SELECT ROW_NUMBER() OVER (ORDER BY global_id) AS row_id, global_id
                    FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
                ) feat ON pred.row_id = feat.row_id
                ORDER BY feat.global_id
            """).result()
            log.info("[_convert] signup_prediction_result 완료")

    # ── Rec (Classifier) ──────────────────────────────────────────────────────
    if REC_MODEL_RESOURCE_NAME:
        tbl = _find_latest_pred_table(bq, "rec_raw_pred")
        if tbl is None:
            log.info("[_convert] rec_raw_pred 없음 — 기존 rec_prediction_result 유지")
        else:
            bq.query(f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.rec_prediction_result` AS
                SELECT
                    feat.global_id,
                    CURRENT_DATE()               AS feature_dt,
                    COALESCE(vip.vip_score, 0.0) AS vip_score,
                    pred.invest_total,
                    pred.push_click_rate,
                    IF(p >= 0.5, 1, 0)           AS predicted_label,
                    [p, ROUND(1.0 - p, 4)]       AS predicted_scores,
                    CURRENT_TIMESTAMP()          AS prediction_time
                FROM (
                    SELECT *,
                        COALESCE(
                            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(prediction), '$[1]') AS FLOAT64),
                            SAFE_CAST(prediction AS FLOAT64), 0.0
                        ) AS p
                    FROM `{PROJECT_ID}.lifesync_ml.{tbl}`
                ) pred
                JOIN (
                    SELECT ROW_NUMBER() OVER (ORDER BY global_id) AS row_id, global_id
                    FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
                ) feat ON pred.row_id = feat.row_id
                LEFT JOIN (
                    SELECT global_id, predicted_scores[SAFE_OFFSET(0)] AS vip_score
                    FROM `{PROJECT_ID}.lifesync_ml.vip_prediction_result`
                ) vip ON feat.global_id = vip.global_id
                ORDER BY feat.global_id
            """).result()
            log.info("[_convert] rec_prediction_result 완료")

    # ── Health (Regressor: prediction = single FLOAT64) ───────────────────────
    if HEALTH_MODEL_RESOURCE_NAME:
        tbl = _find_latest_pred_table(bq, "health_raw_pred")
        if tbl is None:
            log.info("[_convert] health_raw_pred 없음 — 기존 health_prediction_result 유지")
        else:
            bq.query(f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.health_prediction_result` AS
                SELECT
                    feat.global_id,
                    CURRENT_DATE()          AS feature_dt,
                    pred.avg_steps_30d,
                    pred.avg_hr_30d,
                    COALESCE(CAST(feat.stress_avg_30d AS FLOAT64), 0.0) AS stress_avg_30d,
                    CAST(pred.hospital_visit_90d AS INT64) AS hospital_visit_90d,
                    CAST(pred.inactive_days      AS INT64) AS inactive_days,
                    ROUND(100.0 - LEAST(100.0, GREATEST(5.0, COALESCE(
                        SAFE_CAST(pred.prediction AS FLOAT64), 50.0
                    ))), 1) AS predicted_value,
                    CURRENT_TIMESTAMP()     AS prediction_time
                FROM `{PROJECT_ID}.lifesync_ml.{tbl}` pred
                JOIN (
                    SELECT ROW_NUMBER() OVER (ORDER BY global_id) AS row_id, global_id,
                        COALESCE(CAST(health_risk_score AS FLOAT64), 0.0) AS health_risk_score
                    FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
                ) feat ON pred.row_id = feat.row_id
                ORDER BY feat.global_id
            """).result()
            log.info("[_convert] health_prediction_result 완료")

    # 미설정 모델은 mock으로 보완
    _fill_missing_with_mock(bq)
    # raw 예측 테이블 삭제 — lifesync_ml에 *_prediction_result만 남김
    _drop_raw_tables(bq)


def _drop_raw_tables(bq: bigquery.Client):
    """Vertex AI 배치 예측 raw 출력 테이블 삭제."""
    for prefix in ["vip_raw_pred", "signup_raw_pred", "rec_raw_pred", "health_raw_pred"]:
        rows = list(bq.query(f"""
            SELECT table_name
            FROM `{PROJECT_ID}.lifesync_ml.INFORMATION_SCHEMA.TABLES`
            WHERE table_name LIKE '{prefix}%'
        """).result())
        for row in rows:
            bq.delete_table(f"{PROJECT_ID}.lifesync_ml.{row.table_name}", not_found_ok=True)
            log.info("[_drop_raw_tables] 삭제: %s", row.table_name)


def _fill_missing_with_mock(bq: bigquery.Client):
    """미설정 모델에 대해 ai_feature_table 기반 더미 결과 생성."""
    if not VIP_MODEL_RESOURCE_NAME:
        _mock_vip(bq)
    if not SIGNUP_MODEL_RESOURCE_NAME:
        _mock_signup(bq)
    if not REC_MODEL_RESOURCE_NAME:
        _mock_rec(bq)
    if not HEALTH_MODEL_RESOURCE_NAME:
        _mock_health(bq)


def _write_mock_predictions():
    """전체 모델 미설정 시 4개 테이블 전부 더미 생성."""
    bq = bigquery.Client(project=PROJECT_ID)
    _mock_vip(bq)
    _mock_signup(bq)
    _mock_rec(bq)
    _mock_health(bq)
    log.info("[_write_mock_predictions] 완료")


def _mock_vip(bq: bigquery.Client):
    bq.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.vip_prediction_result` AS
        WITH base AS (
            SELECT global_id, CURRENT_DATE() AS feature_dt,
                   COALESCE(CAST(balance_30d_avg AS FLOAT64), 0.0) AS balance_30d_avg,
                   COALESCE(CAST(invest_total    AS FLOAT64), 0.0) AS invest_total,
                   COALESCE(CAST(affiliate_cnt   AS FLOAT64), 0.0) AS affiliate_cnt,
                   ROUND(RAND(), 4) AS p
            FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
        )
        SELECT global_id, feature_dt, balance_30d_avg, invest_total,
               CAST(affiliate_cnt AS INT64) AS affiliate_cnt,
               IF(p >= 0.5, 1, 0)       AS predicted_label,
               [p, ROUND(1.0-p, 4)]     AS predicted_scores,
               CURRENT_TIMESTAMP()      AS prediction_time
        FROM base
        ORDER BY global_id
    """).result()
    log.info("[mock] vip_prediction_result 완료")


def _mock_signup(bq: bigquery.Client):
    bq.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.signup_prediction_result` AS
        WITH base AS (
            SELECT global_id, CURRENT_DATE() AS feature_dt,
                   COALESCE(CAST(balance_30d_avg       AS FLOAT64), 0.0) AS balance_30d_avg,
                   COALESCE(CAST(card_spend_30d         AS FLOAT64), 0.0) AS card_spend_30d,
                   COALESCE(CAST(invest_ratio           AS FLOAT64), 0.0) AS invest_ratio,
                   CAST(login_cnt_30d AS INT64)                           AS login_cnt_30d,
                   COALESCE(CAST(recommend_click_rate   AS FLOAT64), 0.0) AS recommend_click_rate,
                   CAST(affiliate_cnt AS INT64)                           AS affiliate_cnt,
                   ROUND(RAND(), 4) AS p
            FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
        )
        SELECT global_id, feature_dt, balance_30d_avg, card_spend_30d, invest_ratio,
               login_cnt_30d, recommend_click_rate, affiliate_cnt,
               IF(p >= 0.5, 1, 0)       AS predicted_label,
               [p, ROUND(1.0-p, 4)]     AS predicted_scores,
               CURRENT_TIMESTAMP()      AS prediction_time
        FROM base
        ORDER BY global_id
    """).result()
    log.info("[mock] signup_prediction_result 완료")


def _mock_rec(bq: bigquery.Client):
    bq.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.rec_prediction_result` AS
        WITH base AS (
            SELECT feat.global_id, CURRENT_DATE() AS feature_dt,
                   COALESCE(CAST(feat.lifesync_score AS FLOAT64), 0.0) AS lifesync_score,
                   COALESCE(vip.vip_score, 0.0)                        AS vip_score,
                   COALESCE(CAST(feat.invest_total    AS FLOAT64), 0.0) AS invest_total,
                   COALESCE(CAST(feat.push_click_rate AS FLOAT64), 0.0) AS push_click_rate,
                   ROUND(RAND(), 4) AS p
            FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table` feat
            LEFT JOIN (
                SELECT global_id, predicted_scores[SAFE_OFFSET(0)] AS vip_score
                FROM `{PROJECT_ID}.lifesync_ml.vip_prediction_result`
            ) vip ON feat.global_id = vip.global_id
        )
        SELECT global_id, feature_dt, lifesync_score, vip_score, invest_total, push_click_rate,
               IF(p >= 0.5, 1, 0)       AS predicted_label,
               [p, ROUND(1.0-p, 4)]     AS predicted_scores,
               CURRENT_TIMESTAMP()      AS prediction_time
        FROM base
        ORDER BY global_id
    """).result()
    log.info("[mock] rec_prediction_result 완료")


def _mock_health(bq: bigquery.Client):
    bq.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.health_prediction_result` AS
        SELECT global_id,
               CURRENT_DATE()                                              AS feature_dt,
               COALESCE(CAST(avg_steps_30d  AS FLOAT64), 0.0) AS avg_steps_30d,
               COALESCE(CAST(avg_hr_30d     AS FLOAT64), 0.0) AS avg_hr_30d,
               COALESCE(CAST(stress_avg_30d AS FLOAT64), 0.0) AS stress_avg_30d,
               CAST(COALESCE(hospital_visit_90d, 0) AS INT64)             AS hospital_visit_90d,
               CAST(COALESCE(inactive_days, 0)      AS INT64)             AS inactive_days,
               ROUND(100.0 - LEAST(100.0, GREATEST(5.0, COALESCE(
                   CAST(health_risk_score AS FLOAT64), 50.0
               ))), 1) AS predicted_value,
               CURRENT_TIMESTAMP() AS prediction_time
        FROM `{PROJECT_ID}.lifesync_curated.ai_feature_table`
        ORDER BY global_id
    """).result()
    log.info("[mock] health_prediction_result 완료")


_SERVING_SQL = """
    CREATE OR REPLACE TABLE `{project}.lifesync_serving.customer_recommendations` AS
    WITH
    feat AS (
        SELECT
            global_id,
            COALESCE(CAST(lifesync_score AS FLOAT64), 0.0) AS lifesync_score,
            -- 행동 점수 proxy (login_cnt_30d, recommend_click_rate, avg_session_min, push_click_rate)
            (
                CASE WHEN COALESCE(CAST(login_cnt_30d AS FLOAT64), 0.0) >= 5.0 THEN 20 ELSE 0 END +
                CASE WHEN COALESCE(CAST(recommend_click_rate AS FLOAT64), 0.0) > 0.0 THEN 25 ELSE 0 END +
                CASE WHEN COALESCE(CAST(avg_session_min AS FLOAT64), 0.0) >= 10.0 THEN 20 ELSE 0 END +
                CASE WHEN COALESCE(CAST(push_click_rate AS FLOAT64), 0.0) > 0.0 THEN 15 ELSE 0 END
            ) AS behavior_score,
            -- 이탈 위험 점수 proxy (inactive_days, card_drop_ratio, asset_drop_ratio, complaint_flag)
            (
                CASE WHEN COALESCE(CAST(inactive_days AS FLOAT64), 0.0) >= 30.0 THEN 20 ELSE 0 END +
                CASE WHEN COALESCE(CAST(card_drop_ratio AS FLOAT64), 0.0) >= 0.3 THEN 15 ELSE 0 END +
                CASE WHEN COALESCE(CAST(asset_drop_ratio AS FLOAT64), 0.0) >= 0.3 THEN 20 ELSE 0 END +
                CASE WHEN COALESCE(CAST(complaint_flag AS FLOAT64), 0.0) >= 1.0 THEN 25 ELSE 0 END
            ) AS churn_risk
        FROM `{project}.lifesync_curated.ai_feature_table`
    ),
    vip AS (
        SELECT global_id, predicted_scores[SAFE_OFFSET(0)] AS vip_prob
        FROM `{project}.lifesync_ml.vip_prediction_result`
    ),
    signup AS (
        SELECT global_id, predicted_scores[SAFE_OFFSET(0)] AS signup_prob
        FROM `{project}.lifesync_ml.signup_prediction_result`
    ),
    rec AS (
        SELECT global_id, predicted_scores[SAFE_OFFSET(0)] AS rec_prob
        FROM `{project}.lifesync_ml.rec_prediction_result`
    ),
    health AS (
        SELECT global_id, predicted_value AS health_score
        FROM `{project}.lifesync_ml.health_prediction_result`
    ),
    scored AS (
        SELECT
            v.global_id,
            COALESCE(v.vip_prob, 0.0)       AS vip_prob,
            COALESCE(s.signup_prob, 0.0)    AS signup_prob,
            COALESCE(r.rec_prob, 0.0)       AS rec_prob,
            COALESCE(h.health_score, 0.0)   AS health_score,
            COALESCE(f.behavior_score, 0.0) AS behavior_score,
            COALESCE(f.churn_risk, 0.0)     AS churn_risk,
            -- DynamicScore = (BaseScore×0.50) + (VIP×15) + (Signup×10) + (Rec×10)
            --              + (Health×0.10) + (Behavior×0.10) - (Churn×0.05)
            ROUND(
                LEAST(100.0, GREATEST(0.0,
                    COALESCE(f.lifesync_score, 0.0)  * 0.50
                    + COALESCE(v.vip_prob, 0.0)      * 15.0
                    + COALESCE(s.signup_prob, 0.0)   * 10.0
                    + COALESCE(r.rec_prob, 0.0)      * 10.0
                    + COALESCE(h.health_score, 0.0)  * 0.10
                    + COALESCE(f.behavior_score, 0.0) * 0.10
                    - COALESCE(f.churn_risk, 0.0)    * 0.05
                )),
                1
            ) AS dynamic_score
        FROM vip v
        LEFT JOIN signup s ON v.global_id = s.global_id
        LEFT JOIN rec    r ON v.global_id = r.global_id
        LEFT JOIN health h ON v.global_id = h.global_id
        LEFT JOIN feat   f ON v.global_id = f.global_id
    )
    SELECT
        global_id,
        dynamic_score,
        CASE
            WHEN dynamic_score >= 90 THEN 'VIP'
            WHEN dynamic_score >= 80 THEN 'GOLD'
            WHEN dynamic_score >= 70 THEN 'SILVER'
            WHEN dynamic_score >= 60 THEN 'BASIC'
            ELSE 'CARE'
        END AS dynamic_grade,
        -- 추천 엔진: 모델 확률 기반 우선순위 순
        CASE
            WHEN vip_prob >= 0.5    THEN 'PB_CENTER'
            WHEN rec_prob >= 0.5    THEN 'ETF_PRODUCT'
            WHEN signup_prob >= 0.5 THEN 'PREMIUM_CARD'
            WHEN health_score < 50.0 THEN 'HEALTH_CHECKUP'
            WHEN churn_risk >= 20.0  THEN 'RETENTION_COUPON'
            ELSE 'BASIC_SERVICE'
        END AS next_best_action,
        ROUND(vip_prob, 4)      AS vip_prob,
        ROUND(signup_prob, 4)   AS signup_prob,
        ROUND(rec_prob, 4)      AS rec_prob,
        ROUND(health_score, 1)  AS health_score,
        CURRENT_TIMESTAMP()     AS update_time,
        'GCP_lifesync'          AS source,
        CAST(UNIX_SECONDS(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS INT64) AS ttl
    FROM scored
    ORDER BY global_id
"""


_SERVING_VIEWS = {
    "v_user_dashboard": """
        SELECT
            global_id, dynamic_score, dynamic_grade, next_best_action,
            vip_prob, signup_prob, rec_prob, health_score, update_time,
            source, ttl
        FROM `{project}.lifesync_serving.customer_recommendations`
    """,
    "v_customer_summary": """
        SELECT global_id, dynamic_score, dynamic_grade, health_score, update_time
        FROM `{project}.lifesync_serving.customer_recommendations`
    """,
    "v_vip_customer": """
        SELECT global_id, dynamic_score, vip_prob, signup_prob, rec_prob,
               health_score, update_time
        FROM `{project}.lifesync_serving.customer_recommendations`
        WHERE dynamic_grade = 'VIP'
    """,
    "v_recommend_top3": """
        SELECT global_id, dynamic_grade, next_best_action,
               ROUND(rec_prob, 4)     AS rec_prob,
               ROUND(vip_prob, 4)     AS vip_prob,
               ROUND(signup_prob, 4)  AS signup_prob,
               ROUND(health_score, 1) AS health_score,
               update_time
        FROM `{project}.lifesync_serving.customer_recommendations`
        WHERE rec_prob >= 0.5
    """,
}


def _refresh_serving_table():
    bq = bigquery.Client(project=PROJECT_ID)
    bq.query(_SERVING_SQL.format(project=PROJECT_ID)).result()
    log.info("[_refresh_serving_table] customer_recommendations 완료")
    for view_name, view_sql in _SERVING_VIEWS.items():
        bq.query(f"""
            CREATE OR REPLACE VIEW `{PROJECT_ID}.lifesync_serving.{view_name}` AS
            {view_sql.format(project=PROJECT_ID)}
        """).result()
        log.info("[_refresh_serving_table] %s 완료", view_name)
    log.info("[_refresh_serving_table] 전체 완료")


def _write_gcs_marker(path: str):
    gcs = storage.Client(project=PROJECT_ID)
    gcs.bucket(GCS_BUCKET).blob(path).upload_from_string("done", content_type="text/plain")
    log.info("[_write_gcs_marker] gs://%s/%s", GCS_BUCKET, path)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
