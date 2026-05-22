import sys
import json
import boto3
from datetime import datetime, timezone, timedelta

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when

# ── 환경 설정 ─────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_CURATED_BUCKET", "BATCH_DATE"])
JOB_NAME          = args["JOB_NAME"]
S3_CURATED_BUCKET = args.get("S3_CURATED_BUCKET", "lifesync-curated")
BATCH_DATE        = args.get("BATCH_DATE", datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d"))

sc    = SparkContext()
glue  = GlueContext(sc)
spark = glue.spark_session
job   = Job(glue)
job.init(JOB_NAME, args)

spark.sparkContext.setLogLevel("WARN")

# BATCH_DATE는 Glue Job 파라미터(YYYY-MM-DD)로만 전달되며, datetime.strptime으로 포맷 검증
datetime.strptime(BATCH_DATE, "%Y-%m-%d")
print(f"[aurora_sync] BATCH_DATE={BATCH_DATE}")


def _get_aurora_creds() -> dict:
    sm = boto3.client("secretsmanager", region_name="ap-northeast-2")
    return json.loads(
        sm.get_secret_value(SecretId="lifesync/aurora/credentials")["SecretString"]
    )


# ── Aurora JDBC 연결 ──────────────────────────────────────────────────────────
print("[aurora_sync] Fetching Aurora credentials from Secrets Manager")
creds    = _get_aurora_creds()
jdbc_url = (
    f"jdbc:mysql://{creds['host']}:{creds.get('port', 3306)}"
    f"/{creds.get('dbname', 'lifesync360')}"
)
jdbc_props = {
    "user":     creds["username"],
    "password": creds["password"],
    "driver":   "com.mysql.cj.jdbc.Driver",
}

# ── customer_recommend_history 읽기 (배치 날짜 기준 필터) ─────────────────────
# recommended_at 기준 당일 데이터만 추출 (pushdown predicate)
print(f"[aurora_sync] Reading customer_recommend_history for {BATCH_DATE}")

pushdown = f"(SELECT * FROM customer_recommend_history WHERE DATE(recommended_at) = '{BATCH_DATE}') t"  # nosec B608

df = spark.read.jdbc(
    url=jdbc_url,
    table=pushdown,
    properties=jdbc_props,
)

print(f"[aurora_sync] 읽은 행 수: {df.count():,}")

# ── 컬럼 정제 ─────────────────────────────────────────────────────────────────
df = df.select(
    col("hist_id").cast("string"),
    col("global_id").cast("string"),
    col("company_id").cast("string"),
    col("product_id").cast("string"),
    col("dynamic_grade").cast("string"),
    col("dynamic_score").cast("double"),
    col("action_code").cast("string"),
    col("recommended_at").cast("timestamp"),
    # clicked_flag / purchased_flag: Y/N → 1/0 정수 변환
    when(col("clicked_flag") == "Y", lit(1)).otherwise(lit(0)).cast("int").alias("clicked_flag"),
    when(col("purchased_flag") == "Y", lit(1)).otherwise(lit(0)).cast("int").alias("purchased_flag"),
)

df = df.withColumn("dt", lit(BATCH_DATE))

# ── S3 Curated 적재 ───────────────────────────────────────────────────────────
output_path = f"s3://{S3_CURATED_BUCKET}/customer_recommend_history/dt={BATCH_DATE}/"
print(f"[aurora_sync] Writing to {output_path}")

df.write \
  .mode("overwrite") \
  .partitionBy("dt") \
  .parquet(output_path)

print(f"[aurora_sync] 완료. Output: {output_path}")
job.commit()
