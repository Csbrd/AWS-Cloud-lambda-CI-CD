import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when
from pyspark.sql.window import Window

BATCH_DATE = os.environ.get("BATCH_DATE")
if not BATCH_DATE:
    for i, arg in enumerate(sys.argv):
        if arg == "--batch-date" and i + 1 < len(sys.argv):
            BATCH_DATE = sys.argv[i + 1]
            break
if not BATCH_DATE:
    print("ERROR: BATCH_DATE environment variable is required (format: YYYYMMDD)")
    sys.exit(1)

S3_CURATED_BUCKET = os.environ.get("S3_CURATED_BUCKET", "lifesync-curated")

date_formatted = f"{BATCH_DATE[:4]}-{BATCH_DATE[4:6]}-{BATCH_DATE[6:8]}"

spark = SparkSession.builder \
    .appName("vip_mart") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[vip_mart] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

customer360_path = f"s3://{S3_CURATED_BUCKET}/customer_360_profile/dt={date_formatted}/"
score_mart_path  = f"s3://{S3_CURATED_BUCKET}/score_mart/dt={date_formatted}/"

print(f"[vip_mart] Reading customer_360_profile from {customer360_path}")
df_c360 = spark.read.parquet(customer360_path)

print(f"[vip_mart] Reading score_mart from {score_mart_path}")
df_score = spark.read.parquet(score_mart_path).select(
    "global_id", "lifesync_score", "customer_grade",
    "pb_score", "churn_score", "health_score",
)

print("[vip_mart] Joining customer360 and score_mart")
df = df_c360.join(df_score, on="global_id", how="inner")

print("[vip_mart] Filtering VIP/GOLD customers")
vip_df = df.filter(col("customer_grade").isin("VIP", "GOLD"))

# total_asset
vip_df = vip_df.withColumn(
    "total_asset",
    col("latest_balance") + col("invest_total"),
)

# vip_score_final = lifesync_score
vip_df = vip_df.withColumn("vip_score_final", col("lifesync_score"))

# vip_level: 총자산 구간 기반 PB 등급
vip_df = vip_df.withColumn(
    "vip_level",
    when(col("total_asset") >= 300_000_000, lit("PLATINUM"))
    .when(col("total_asset") >= 100_000_000, lit("GOLD_VIP"))
    .otherwise(lit("STANDARD_VIP"))
)

# priority_rank: customer_grade 내 lifesync_score 내림차순
window_spec = Window.partitionBy("customer_grade").orderBy(col("lifesync_score").desc())
vip_df = vip_df.withColumn("priority_rank", F.rank().over(window_spec))

# recommended_action: 우선순위 기반 Next Best Action
vip_df = vip_df.withColumn(
    "recommended_action",
    when(col("customer_grade") == "VIP",   lit("PB_CENTER"))
    .when(col("churn_score") >= 50,         lit("RETENTION_COUPON"))
    .when(col("pb_score") >= 70,            lit("ETF_PRODUCT"))
    .otherwise(lit("PREMIUM_CARD"))
)

vip_mart = vip_df.select(
    col("global_id"),
    col("total_asset"),
    col("lifesync_score"),
    col("vip_score_final"),
    col("vip_level"),
    col("priority_rank"),
    col("recommended_action"),
    col("customer_grade"),
    col("health_score"),
    col("pb_score"),
    col("churn_score"),
    col("dt"),
)

output_path = f"s3://{S3_CURATED_BUCKET}/vip_mart/dt={date_formatted}/"
print(f"[vip_mart] Writing output to {output_path}")

vip_mart.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[vip_mart] Job completed successfully. Output: {output_path}")
spark.stop()
