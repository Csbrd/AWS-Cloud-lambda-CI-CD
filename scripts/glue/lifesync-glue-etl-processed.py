import os
import sys
import time
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, DateType, DoubleType, StructType, StructField

# ── 계열사별 설정 ──────────────────────────────────────────────────────────────
CONFIGS = {
    "bank": {
        "pk_cols":     ["bank_id", "transaction_date"],
        "rename_cols": {
            "amount":         "tx_amount",
            "balance_after":  "balance",
            "transaction_dt": "transaction_date",
        },
        "keep_cols":   ["bank_id", "global_id", "balance",
                        "tx_amount", "transaction_type", "transaction_date"],
        "schema": [
            ("bank_id",          StringType()),
            ("global_id",        StringType()),
            ("balance_after",    LongType()),
            ("amount",           LongType()),
            ("transaction_type", StringType()),
            ("transaction_dt",   DateType()),
        ],
    },
    "card": {
        "pk_cols":     ["card_id", "transaction_date"],
        "rename_cols": {
            "amount":      "spend_amount",
            "approval_dt": "transaction_date",
        },
        "keep_cols":   ["card_id", "global_id", "spend_amount",
                        "merchant_category", "transaction_date"],
        "schema": [
            ("card_id",           StringType()),
            ("global_id",         StringType()),
            ("amount",            LongType()),
            ("merchant_category", StringType()),
            ("approval_dt",       DateType()),
        ],
    },
    "securities": {
        "pk_cols":     ["sec_id", "transaction_date"],
        "rename_cols": {
            "trade_amount": "invest_amount",
            "trade_type":   "product_type",
            "trade_dt":     "transaction_date",
        },
        "keep_cols":   ["sec_id", "global_id", "invest_amount",
                        "stock_code", "product_type", "transaction_date"],
        "schema": [
            ("sec_id",       StringType()),
            ("global_id",    StringType()),
            ("trade_amount", LongType()),
            ("stock_code",   StringType()),
            ("trade_type",   StringType()),
            ("trade_dt",     DateType()),
        ],
    },
    "insurance": {
        "pk_cols":     ["ins_id"],
        "rename_cols": {
            "amount": "premium_amount",
        },
        "keep_cols": ["ins_id", "global_id", "premium_amount", "event_dt"],
        "schema": [
            ("ins_id",         StringType()),
            ("global_id",      StringType()),
            ("amount",         LongType()),
            ("event_dt",       StringType()),
        ],
    },
    "online_insurance": {
        "pk_cols":     ["iin_id"],
        "rename_cols": {
            "amount":   "premium_amount",
            "event_dt": "transaction_date",
        },
        "keep_cols": ["iin_id", "global_id", "premium_amount", "transaction_date"],
        "schema": [
            ("iin_id",    StringType()),
            ("global_id", StringType()),
            ("amount",    LongType()),
            ("event_dt",  DateType()),
        ],
    },
    "healthcare": {
        "pk_cols":   ["hc_id"],
        "keep_cols": ["hc_id", "global_id", "diet_score", "sleep_score", "calories", "bmi"],
        "schema": [
            ("hc_id",       StringType()),
            ("global_id",   StringType()),
            ("diet_score",  LongType()),
            ("sleep_score", LongType()),
            ("calories",    LongType()),
            ("bmi",         DoubleType()),
        ],
    },
    "hospital": {
        "pk_cols":   ["hospital_id", "visit_id"],
        "keep_cols": ["hospital_id", "global_id", "visit_id", "dept"],
        "schema": [
            ("hospital_id", StringType()),
            ("global_id",   StringType()),
            ("visit_id",    StringType()),
            ("dept",        StringType()),
        ],
    },
    "wearable": {
        "pk_cols":   ["global_id", "record_date"],
        "keep_cols": ["global_id", "heart_rate", "steps",
                      "stress_score", "spo2_pct", "record_date"],
        "schema": [
            ("global_id",    StringType()),
            ("heart_rate",   LongType()),
            ("steps",        LongType()),
            ("stress_score", LongType()),
            ("spo2_pct",     LongType()),
            ("record_date",  DateType()),
        ],
    },
}

SUBSIDIARIES = list(CONFIGS.keys())

RAW_BUCKET  = "lifesync-raw"
PROC_BUCKET = "lifesync-processed"
KST         = timezone(timedelta(hours=9))

# ── Glue Job 초기화 ────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)


def _optional_arg(name, default=None):
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


date_str  = _optional_arg("date", datetime.now(KST).strftime("%Y-%m-%d"))
SKIP_EMR  = (_optional_arg("skip-emr", "false") or "false").lower() == "true"
s3_client = boto3.client("s3")

EMR_APP_ID   = os.environ.get("EMR_APP_ID")        or _optional_arg("emr_app_id",        "")
EMR_ROLE_ARN = os.environ.get("EMR_ROLE_ARN")       or _optional_arg("emr_role_arn",       "")
S3_SCRIPTS   = os.environ.get("S3_SCRIPT_BASE")     or _optional_arg("S3_SCRIPT_BASE",     "s3://lifesync-script-bucket/emr")
S3_CURATED   = os.environ.get("S3_CURATED_BUCKET")  or _optional_arg("S3_CURATED_BUCKET",  "lifesync-curated")

# ── consent 스냅샷 1회 로드 (전 계열사 공통 사용) ─────────────────────────────
_consent_schema = StructType([
    StructField("global_id",       StringType(), True),
    StructField("domain",          StringType(), True),
    StructField("consent_flag",    StringType(), True),
    StructField("consent_version", StringType(), True),
    StructField("consent_dt",      StringType(), True),
    StructField("revoke_dt",       StringType(), True),
])
consent_ids = (
    spark.read.schema(_consent_schema)
         .json(f"s3://{RAW_BUCKET}/consent/")
         .filter(
             (F.col("consent_flag") == "Y") &
             (F.col("revoke_dt").isNull() | (F.col("revoke_dt") == "None"))
         )
         .select("global_id")
         .distinct()
         .cache()
)
print(f"[glue-etl] consent 로드 완료 date={date_str}")

# ── 8개 계열사 순차 처리 ──────────────────────────────────────────────────────
for SOURCE in SUBSIDIARIES:
    cfg = CONFIGS[SOURCE]
    print(f"[{SOURCE}] 처리 시작")

    # 1. S3 Raw JSON 읽기 (Job Bookmark 활성화)
    _s3_path = f"s3://{RAW_BUCKET}/{SOURCE}/dt={date_str}/"
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiline": False},
        connection_options={
            "paths": [_s3_path],
            "recurse": True,
        },
        transformation_ctx=f"read_{SOURCE}",
    )
    raw_df = dyf.toDF()

    # 래핑된 JSON 포맷 처리 {"source":..., "records": [...]}
    if "records" in raw_df.columns:
        raw_df = raw_df.select(F.explode("records").alias("rec")).select("rec.*")

    # wearable: payload 중첩 구조 펼치기 + event_time → record_date
    if SOURCE == "wearable" and "payload" in raw_df.columns:
        raw_df = raw_df.select(
            F.col("global_id"),
            F.to_date(F.col("event_time")).alias("record_date"),
            F.col("payload.heart_rate").alias("heart_rate"),
            F.col("payload.steps").alias("steps"),
            F.col("payload.stress_score").alias("stress_score"),
            F.col("payload.spo2_pct").alias("spo2_pct"),
        )

    # 빈 DynamicFrame 처리 — S3 경로에 데이터 없거나 스키마 추론 실패 시 명시적 실패
    if not raw_df.columns:
        raise RuntimeError(
            f"[{SOURCE}] S3 경로에 데이터 없음 또는 스키마 추론 실패: "
            f"s3://{RAW_BUCKET}/{SOURCE}/dt={date_str}/ — "
            "Raw 데이터 생성 여부 및 경로/날짜 형식을 확인하세요."
        )

    if "global_id" not in raw_df.columns:
        raise RuntimeError(
            f"[{SOURCE}] global_id 컬럼 없음. 실제 컬럼: {raw_df.columns} — "
            "S3 파일 포맷이 예상과 다릅니다. records 래핑 구조 또는 wearable payload 구조를 확인하세요."
        )

    # 2. 동의 고객 필터링
    filtered_df = raw_df.join(consent_ids, on="global_id", how="inner")

    # 3. 스키마 정규화
    normalized_df = filtered_df
    for col_name, col_type in cfg["schema"]:
        normalized_df = normalized_df.withColumn(col_name, F.col(col_name).cast(col_type))

    # 3.5 컬럼명 표준화
    for old_name, new_name in cfg.get("rename_cols", {}).items():
        normalized_df = normalized_df.withColumnRenamed(old_name, new_name)

    # 4. PII 제거 + 필요 컬럼 선택
    selected_df = normalized_df.select(cfg["keep_cols"])

    # 5. 중복 제거
    if SOURCE == "wearable":
        # 같은 날 여러 이벤트 → 일별 합산(steps) / 평균(나머지)
        deduped_df = selected_df.groupBy("global_id", "record_date").agg(
            F.sum("steps").cast(LongType()).alias("steps"),
            F.avg("heart_rate").cast(LongType()).alias("heart_rate"),
            F.avg("stress_score").cast(LongType()).alias("stress_score"),
            F.avg("spo2_pct").cast(LongType()).alias("spo2_pct"),
        )
    elif SOURCE in ("bank", "card", "securities"):
        # 같은 날 복수 거래는 정상 데이터 — 완전 동일 행만 제거
        deduped_df = selected_df.dropDuplicates()
    else:
        deduped_df = selected_df.dropDuplicates(cfg["pk_cols"])

    # 6. S3 Processed Parquet 저장 (Snappy 압축, dt= 파티션)
    deduped_df = deduped_df.withColumn("dt", F.lit(date_str))

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    deduped_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .option("compression", "snappy") \
        .parquet(f"s3://{PROC_BUCKET}/{SOURCE}/")

    # 7. 마커 파일 생성
    s3_client.put_object(
        Bucket=PROC_BUCKET,
        Key=f"_markers/{date_str}/{SOURCE}.done",
        Body=b"done",
    )
    print(f"[{SOURCE}] 완료 — 마커 생성: _markers/{date_str}/{SOURCE}.done")

# ── EMR 6개 Step 순차 제출 ────────────────────────────────────────────────────
if SKIP_EMR:
    print(f"[glue-etl] skip-emr=true — EMR 제출 생략 (date={date_str})")
else:
    print("[glue-etl] 전체 계열사 처리 완료 — EMR Job 순차 제출 시작")
    batch_date = date_str.replace("-", "")
    emr = boto3.client("emr-serverless", region_name="ap-northeast-2")

    emr_jobs = [
        ("customer360",      "customer360.py"),
        ("score_mart",       "score_mart.py"),
        ("vip_mart",         "vip_mart.py"),
        ("recommendation",   "recommendation.py"),
        ("health_mart",      "health_mart.py"),
        ("ai_feature_table", "ai_feature_table.py"),
    ]

    TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}
    POLL_INTERVAL   = 30

    for job_name, script_file in emr_jobs:
        response = emr.start_job_run(
            applicationId=EMR_APP_ID,
            executionRoleArn=EMR_ROLE_ARN,
            name=f"lifesync-{job_name}-{batch_date}",
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"{S3_SCRIPTS}/{script_file}",
                    "entryPointArguments": ["--batch-date", batch_date],
                    "sparkSubmitParameters": (
                        "--conf spark.executor.cores=2 "
                        "--conf spark.executor.memory=4g"
                    ),
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": "s3://lifesync-script-bucket/emr-logs/"
                    }
                }
            },
        )
        job_run_id = response["jobRunId"]
        print(f"[emr] {job_name} 제출 완료 jobRunId={job_run_id}")

        while True:
            status = emr.get_job_run(
                applicationId=EMR_APP_ID,
                jobRunId=job_run_id,
            )["jobRun"]["state"]

            if status in TERMINAL_STATES:
                print(f"[emr] {job_name} 종료 state={status}")
                if status != "SUCCESS":
                    raise RuntimeError(f"EMR {job_name} 실패: state={status}")
                break

            print(f"[emr] {job_name} 진행 중 state={status} — {POLL_INTERVAL}초 대기")
            time.sleep(POLL_INTERVAL)

job.commit()
