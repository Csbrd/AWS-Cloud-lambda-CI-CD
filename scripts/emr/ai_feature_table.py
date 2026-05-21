import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, greatest, coalesce

BATCH_DATE = os.environ.get("BATCH_DATE")
if not BATCH_DATE:
    for i, arg in enumerate(sys.argv):
        if arg == "--batch-date" and i + 1 < len(sys.argv):
            BATCH_DATE = sys.argv[i + 1]
            break
if not BATCH_DATE:
    print("ERROR: BATCH_DATE environment variable is required (format: YYYYMMDD)")
    sys.exit(1)

S3_PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET", "lifesync-processed")
S3_CURATED_BUCKET   = os.environ.get("S3_CURATED_BUCKET",   "lifesync-curated")

date_formatted = f"{BATCH_DATE[:4]}-{BATCH_DATE[4:6]}-{BATCH_DATE[6:8]}"

spark = SparkSession.builder \
    .appName("ai_feature_table") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[ai_feature_table] BATCH_DATE={BATCH_DATE}")

df_c360  = spark.read.parquet(f"s3://{S3_CURATED_BUCKET}/customer_360_profile/dt={date_formatted}/")
df_score = spark.read.parquet(f"s3://{S3_CURATED_BUCKET}/score_mart/dt={date_formatted}/")
df_hmart = spark.read.parquet(f"s3://{S3_CURATED_BUCKET}/health_mart/dt={date_formatted}/") \
               .select("global_id", "avg_hr", "stress")

df = df_c360 \
    .join(df_score.select("global_id", "lifesync_score", "customer_grade"),
          on="global_id", how="left") \
    .join(df_hmart, on="global_id", how="left")

df = df.fillna({
    "lifesync_score": 0.0, "customer_grade": "CARE",
})

# ── 금융 Feature ──────────────────────────────────────────────────────────────
df = df.withColumn("balance_30d_avg",  col("latest_balance"))
df = df.withColumn("asset_growth_90d", F.rand(seed=11) * 0.30 - 0.05)
df = df.withColumn("card_spend_30d",   col("card_total_spend"))

total_assets = col("latest_balance") + col("invest_total") + col("insurance_premium") * lit(12.0)
df = df.withColumn(
    "invest_ratio",
    when(total_assets > 0, col("invest_total") / total_assets).otherwise(lit(0.0))
)
df = df.withColumn("etf_ratio",
    when(col("invest_total") > 0, F.rand(seed=12) * 0.40).otherwise(lit(0.0)))
df = df.withColumn("policy_cnt",
    (col("insurance_count") + col("online_insurance_count")).cast("double"))

# ── 건강 Feature ──────────────────────────────────────────────────────────────
df = df.withColumn("avg_steps_30d",      F.coalesce(col("avg_steps").cast("double"), lit(0.0)))
df = df.withColumn("avg_hr_30d",         F.coalesce(col("avg_hr").cast("double"),    lit(0.0)))
df = df.withColumn("stress_avg_30d",     F.coalesce(col("stress").cast("double"),    lit(40.0)))
df = df.withColumn("hospital_visit_90d", col("hospital_visit_count").cast("double"))
df = df.withColumn("health_risk_score",  greatest(lit(0.0), lit(100.0) - col("health_score")))
df = df.withColumn("step_growth_30d",    F.rand(seed=13) * 0.20 - 0.05)

# ── 행동 Feature ──────────────────────────────────────────────────────────────
_login_base = (
    when(col("customer_grade") == "VIP",     lit(17.0))
    .when(col("customer_grade") == "GOLD",   lit(13.0))
    .when(col("customer_grade") == "SILVER", lit(9.0))
    .when(col("customer_grade") == "BASIC",  lit(5.0))
    .otherwise(lit(2.0))
)
df = df.withColumn("login_cnt_30d",
    F.greatest(lit(0.0), _login_base + (F.rand(seed=21) * 4 - 2)).cast("double"))
df = df.withColumn("avg_session_min",
    (F.rand(seed=22) * 30).cast("double"))
df = df.withColumn("push_click_rate",
    when(F.rand(seed=23) > 0.30, F.rand(seed=24) * 0.50).otherwise(lit(0.0)))
df = df.withColumn("recommend_click_rate",
    when(F.rand(seed=25) > 0.50, F.rand(seed=26) * 0.40).otherwise(lit(0.0)))
df = df.withColumn("last_active_days",
    (F.rand(seed=27) * 45).cast("double"))

# ── 관계 Feature ──────────────────────────────────────────────────────────────
affiliate_expr = (
    when(col("bank_tx_count")          > 0, lit(1)).otherwise(lit(0)) +
    when(col("card_tx_count")          > 0, lit(1)).otherwise(lit(0)) +
    when(col("invest_product_count")   > 0, lit(1)).otherwise(lit(0)) +
    when(col("insurance_count")        > 0, lit(1)).otherwise(lit(0)) +
    when(col("online_insurance_count") > 0, lit(1)).otherwise(lit(0)) +
    when(col("hospital_visit_count")   > 0, lit(1)).otherwise(lit(0))
)
df = df.withColumn("affiliate_cnt",      affiliate_expr.cast("double"))
df = df.withColumn("consent_ratio",      lit(0.5))
df = df.withColumn("membership_days",    lit(365.0))
df = df.withColumn("cross_product_score",
    (col("invest_product_count") + col("insurance_count") + col("online_insurance_count")).cast("double"))

# ── 성장 Feature ──────────────────────────────────────────────────────────────
_growth_base = (
    when(col("customer_grade") == "VIP",     lit(0.10))
    .when(col("customer_grade") == "GOLD",   lit(0.07))
    .when(col("customer_grade") == "SILVER", lit(0.04))
    .when(col("customer_grade") == "BASIC",  lit(0.01))
    .otherwise(lit(-0.02))
)
df = df.withColumn("spend_growth_90d",    _growth_base + (F.rand(seed=31) * 0.10 - 0.05))
df = df.withColumn("invest_growth_90d",   F.rand(seed=32) * 0.25 - 0.05)
df = df.withColumn("wellness_growth_30d", F.rand(seed=33) * 0.20 - 0.05)

# ── Risk Feature ──────────────────────────────────────────────────────────────
_inactive_threshold = (
    when(col("customer_grade") == "VIP",     lit(0.97))
    .when(col("customer_grade") == "GOLD",   lit(0.93))
    .when(col("customer_grade") == "SILVER", lit(0.89))
    .when(col("customer_grade") == "BASIC",  lit(0.85))
    .otherwise(lit(0.75))
)
_inactive_max = (
    when(col("customer_grade") == "VIP",     lit(20.0))
    .when(col("customer_grade") == "GOLD",   lit(35.0))
    .when(col("customer_grade") == "SILVER", lit(55.0))
    .when(col("customer_grade") == "BASIC",  lit(70.0))
    .otherwise(lit(90.0))
)
df = df.withColumn("inactive_days",
    when(F.rand(seed=41) > _inactive_threshold, F.rand(seed=42) * _inactive_max)
    .otherwise(lit(0.0)))
df = df.withColumn("card_drop_ratio",
    when(F.rand(seed=43) > 0.90, F.rand(seed=44) * 0.80).otherwise(lit(0.0)))
df = df.withColumn("asset_drop_ratio",
    when(F.rand(seed=45) > 0.92, F.rand(seed=46) * 0.60).otherwise(lit(0.0)))
df = df.withColumn("complaint_flag",
    when(F.rand(seed=47) > 0.97, lit(1.0)).otherwise(lit(0.0)))

# ── Label ─────────────────────────────────────────────────────────────────────
df = df.withColumn(
    "vip_label",
    when((col("customer_grade").isin("VIP", "GOLD")) & (col("wearable_flag") == "Y"), lit(1))
    .otherwise(lit(0))
)
df = df.withColumn("churn_label",
    when(col("inactive_days") > 30, lit(1)).otherwise(lit(0)))
df = df.withColumn("product_purchase_label",
    when(col("invest_ratio") > 0.3, lit(1)).otherwise(lit(0)))

df = df.withColumn("dt", lit(date_formatted))

ai_feature = df.select(
    col("global_id"),
    col("lifesync_score"),
    # 금융
    col("balance_30d_avg"), col("asset_growth_90d"), col("card_spend_30d"),
    col("invest_total"),    col("invest_ratio"),     col("etf_ratio"),     col("policy_cnt"),
    # 건강
    col("avg_steps_30d"), col("avg_hr_30d"), col("stress_avg_30d"),
    col("hospital_visit_90d"), col("health_risk_score"), col("step_growth_30d"),
    # 행동
    col("login_cnt_30d"), col("avg_session_min"), col("push_click_rate"),
    col("recommend_click_rate"), col("last_active_days"),
    # 관계
    col("affiliate_cnt"), col("consent_ratio"), col("membership_days"), col("cross_product_score"),
    # 성장
    col("spend_growth_90d"), col("invest_growth_90d"), col("wellness_growth_30d"),
    # Risk
    col("inactive_days"), col("card_drop_ratio"), col("asset_drop_ratio"), col("complaint_flag"),
    # Label
    col("vip_label"), col("churn_label"), col("product_purchase_label"),
    col("dt"),
)

output_path = f"s3://{S3_CURATED_BUCKET}/ai_feature_table/"
print(f"[ai_feature_table] Writing to {output_path}")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
ai_feature.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print("[ai_feature_table] Completed successfully")
spark.stop()
