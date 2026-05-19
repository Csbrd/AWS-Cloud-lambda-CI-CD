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
# rename_cols: raw 컬럼명 → EMR 표준 컬럼명 (Glue 출력 시 적용)
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
        "keep_cols": ["hc_id", "global_id", "bmi", "health_score"],
        "schema": [
            ("hc_id",       StringType()),
            ("global_id",   StringType()),
            ("bmi",         DoubleType()),
            ("health_score", LongType()),
        ],
    },
    "hospital": {
        "pk_cols":     ["hos_id", "visit_date"],
        "rename_cols": {
            "cost":     "treatment_cost",
            "visit_dt": "visit_date",
        },
        "keep_cols":   ["hos_id", "global_id", "visit_date",
                        "diagnosis_code", "treatment_cost"],
        "schema": [
            ("hos_id",         StringType()),
            ("global_id",      StringType()),
            ("visit_dt",       DateType()),
            ("diagnosis_code", StringType()),
            ("cost",           LongType()),
        ],
    },
    "wearable": {
        "pk_cols":   ["global_id", "record_date"],
        "keep_cols": ["global_id", "heart_rate", "steps",
                      "sleep_hours", "stress_score", "wellness_score", "record_date"],
        "schema": [
            ("global_id",      StringType()),
            ("heart_rate",     LongType()),
            ("steps",          LongType()),
            ("sleep_hours",    DoubleType()),
            ("stress_score",   LongType()),
            ("wellness_score", LongType()),
            ("record_date",    DateType()),
        ],
    },
}

RAW_BUCKET  = "lifesync-raw"
PROC_BUCKET = "lifesync-processed"
KST         = timezone(timedelta(hours=9))

# ── Glue Job 초기화 ────────────────────────────────────────────────────────────
args   = getResolvedOptions(sys.argv, ["JOB_NAME", "source"])
SOURCE = args["source"]

if SOURCE not in CONFIGS:
    raise ValueError(f"Unknown source '{SOURCE}'. Valid: {list(CONFIGS.keys())}")

cfg = CONFIGS[SOURCE]

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
s3_client = boto3.client("s3")

# ── 1. S3 Raw JSON 읽기 ────────────────────────────────────────────────────────
# lifesync-identity-enricher Lambda가 global_id 매핑 후 원본 경로에 덮어쓰기 완료
raw_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f"s3://{RAW_BUCKET}/{SOURCE}/dt={date_str}/"]},
    format="json",
    transformation_ctx=f"{SOURCE}_raw_src",
).toDF()

# 래핑된 JSON 포맷 처리 {"source":..., "records": [...]}
if "records" in raw_df.columns:
    raw_df = raw_df.select(F.explode("records").alias("rec")).select("rec.*")

# ── 2. consent 스냅샷 읽기 + 동의 고객 필터링 ────────────────────────────────
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
         .json(f"s3://{RAW_BUCKET}/consent/dt={date_str}/")
         .filter(
             (F.col("consent_flag") == "Y") &
             (F.col("revoke_dt").isNull() | (F.col("revoke_dt") == "None"))
         )
         .select("global_id")
         .distinct()
)

# global_id inner join → 동의 고객만 통과
filtered_df = raw_df.join(consent_ids, on="global_id", how="inner")

# ── 3. 스키마 정규화 ───────────────────────────────────────────────────────────
normalized_df = filtered_df
for col_name, col_type in cfg["schema"]:
    normalized_df = normalized_df.withColumn(col_name, F.col(col_name).cast(col_type))

# ── 3.5 컬럼명 표준화 (raw명 → EMR 표준명) ───────────────────────────────────
for old_name, new_name in cfg.get("rename_cols", {}).items():
    normalized_df = normalized_df.withColumnRenamed(old_name, new_name)

# ── 4. PII 제거 + 필요 컬럼만 선택 ───────────────────────────────────────────
selected_df = normalized_df.select(cfg["keep_cols"])

# ── 5. 중복 제거 ───────────────────────────────────────────────────────────────
deduped_df = selected_df.dropDuplicates(cfg["pk_cols"])

# ── 6. S3 Processed에 Parquet 저장 (Snappy 압축, dt= 파티션) ──────────────────
# EMR이 읽는 경로: s3://lifesync-processed/{source}/dt={date_str}/
deduped_df = deduped_df.withColumn("dt", F.lit(date_str))

deduped_df.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .option("compression", "snappy") \
    .parquet(f"s3://{PROC_BUCKET}/{SOURCE}/")

# ── 7. 마커 파일 생성 → EMR 트리거 감지용 ────────────────────────────────────
SUBSIDIARIES = [
    "bank", "card", "securities", "insurance",
    "online_insurance", "healthcare", "hospital", "wearable",
]

EMR_APP_ID   = os.environ.get("EMR_APP_ID", "")
EMR_ROLE_ARN = os.environ.get("EMR_ROLE_ARN", "")
S3_SCRIPTS   = os.environ.get("S3_SCRIPT_BASE", "s3://lifesync-script-bucket/emr")
S3_CURATED   = os.environ.get("S3_CURATED_BUCKET", "lifesync-curated")

# ── 7. 마커 파일 생성 ─────────────────────────────────────────────────────────
s3_client.put_object(
    Bucket=PROC_BUCKET,
    Key=f"_markers/{date_str}/{SOURCE}.done",
    Body=b"done",
)
print(f"[{SOURCE}] 마커 파일 생성 완료: _markers/{date_str}/{SOURCE}.done")

# ── 8. 8개 마커 확인 → 전부 완료 시 EMR 트리거 ───────────────────────────────
def _all_markers_done(date: str) -> bool:
    for sub in SUBSIDIARIES:
        try:
            s3_client.head_object(Bucket=PROC_BUCKET, Key=f"_markers/{date}/{sub}.done")
        except s3_client.exceptions.ClientError:
            return False
    return True

if _all_markers_done(date_str):
    print(f"[{SOURCE}] 8개 마커 모두 확인 — EMR Job 순차 제출 시작")
    batch_date = date_str.replace("-", "")
    emr = boto3.client("emr-serverless", region_name="ap-northeast-2")

    emr_jobs = [
        ("customer360",      "customer360.py"),
        ("score_mart",       "score_mart.py"),
        ("ai_feature_table", "ai_feature_table.py"),
        ("vip_mart",         "vip_mart.py"),
        ("recommendation",   "recommendation.py"),
        ("health_mart",      "health_mart.py"),
    ]

    TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}
    POLL_INTERVAL   = 30  # seconds

    for job_name, script_file in emr_jobs:
        response = emr.start_job_run(
            applicationId=EMR_APP_ID,
            executionRoleArn=EMR_ROLE_ARN,
            name=f"lifesync-{job_name}-{batch_date}",
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"{S3_SCRIPTS}/{script_file}",
                    "sparkSubmitParameters": (
                        "--conf spark.executor.cores=2 "
                        "--conf spark.executor.memory=4g"
                    ),
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{S3_CURATED}/emr-logs/"
                    }
                }
            },
            executionTimeoutMinutes=60,
        )
        job_run_id = response["jobRunId"]
        print(f"[{SOURCE}] EMR {job_name} 제출 완료 jobRunId={job_run_id}")

        # 완료될 때까지 polling 후 다음 Job 제출
        import time
        while True:
            status = emr.get_job_run(
                applicationId=EMR_APP_ID,
                jobRunId=job_run_id,
            )["jobRun"]["state"]

            if status in TERMINAL_STATES:
                print(f"[{SOURCE}] EMR {job_name} 종료 state={status}")
                if status != "SUCCESS":
                    raise RuntimeError(f"EMR {job_name} 실패: state={status}")
                break

            print(f"[{SOURCE}] EMR {job_name} 진행 중 state={status} — {POLL_INTERVAL}초 대기")
            time.sleep(POLL_INTERVAL)
else:
    print(f"[{SOURCE}] 마커 미완료 계열사 있음 — EMR 대기 중")

job.commit()
