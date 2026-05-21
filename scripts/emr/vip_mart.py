import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, least, greatest
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
df_c360 = spark.read.parquet(customer360_path).select(
    "global_id",
    "latest_balance", "invest_total",
    "insurance_premium", "online_insurance_premium",
    "card_total_spend", "invest_product_count",
    "bank_tx_count", "card_tx_count",
    "insurance_count", "online_insurance_count", "hospital_visit_count",
    "avg_steps",
)

print(f"[vip_mart] Reading score_mart from {score_mart_path}")
df_score = spark.read.parquet(score_mart_path).select(
    "global_id", "lifesync_score", "customer_grade",
    "pb_score", "churn_score", "health_score",
)

print("[vip_mart] Joining customer360 and score_mart")
df = df_c360.join(df_score, on="global_id", how="inner")

df = df.fillna({
    "latest_balance": 0.0, "invest_total": 0.0,
    "insurance_premium": 0.0, "online_insurance_premium": 0.0,
    "card_total_spend": 0.0, "invest_product_count": 0,
    "bank_tx_count": 0, "card_tx_count": 0,
    "insurance_count": 0, "online_insurance_count": 0, "hospital_visit_count": 0,
    "avg_steps": 0.0,
    "lifesync_score": 0.0, "customer_grade": "CARE",
    "pb_score": 10.0, "churn_score": 0.0, "health_score": 50.0,
})

# ── total_asset ───────────────────────────────────────────────────────────────
# total_asset = latest_balance + invest_total + premium_total
df = df.withColumn("premium_total",
    col("insurance_premium") + col("online_insurance_premium"))
df = df.withColumn("total_asset",
    col("latest_balance") + col("invest_total") + col("premium_total"))

# ── 자산점수 (max 30) ─────────────────────────────────────────────────────────
asset_s = (when(col("total_asset") >= 500_000_000, lit(30))
           .when(col("total_asset") >= 300_000_000, lit(25))
           .when(col("total_asset") >= 100_000_000, lit(20))
           .when(col("total_asset") >= 70_000_000,  lit(15))
           .when(col("total_asset") >= 50_000_000,  lit(10))
           .otherwise(lit(5)))
df = df.withColumn("asset_score", asset_s.cast("double"))

# ── 수익 기여 점수 (max 20) ───────────────────────────────────────────────────
# 1) 카드 소비 (max 10)
card_rev_s = (when(col("card_total_spend") >= 5_000_000, lit(10))
              .when(col("card_total_spend") >= 3_000_000, lit(7))
              .otherwise(lit(2)))
df = df.withColumn("card_rev_score", card_rev_s.cast("double"))

# 2) 보험료 premium_total (max 5)
ins_rev_s = (when(col("premium_total") >= 3_000_000, lit(5))
             .otherwise(lit(1)))
df = df.withColumn("ins_rev_score", ins_rev_s.cast("double"))

# 3) 투자 거래 trade_cnt = invest_product_count (max 5)
trade_s = (when(col("invest_product_count") >= 10, lit(5))
           .when(col("invest_product_count") >= 3,  lit(3))
           .otherwise(lit(1)))
df = df.withColumn("trade_score", trade_s.cast("double"))

df = df.withColumn("revenue_score",
    col("card_rev_score") + col("ins_rev_score") + col("trade_score"))

# ── 관계 점수 (max 15) ────────────────────────────────────────────────────────
affiliate_expr = (
    when(col("bank_tx_count")          > 0, lit(1)).otherwise(lit(0)) +
    when(col("card_tx_count")          > 0, lit(1)).otherwise(lit(0)) +
    when(col("invest_product_count")   > 0, lit(1)).otherwise(lit(0)) +
    when(col("insurance_count")        > 0, lit(1)).otherwise(lit(0)) +
    when(col("online_insurance_count") > 0, lit(1)).otherwise(lit(0)) +
    when(col("hospital_visit_count")   > 0, lit(1)).otherwise(lit(0))
)
df = df.withColumn("affiliate_cnt", affiliate_expr)

rel_s = (when(col("affiliate_cnt") >= 5, lit(15))
         .when(col("affiliate_cnt") >= 4, lit(12))
         .when(col("affiliate_cnt") >= 3, lit(8))
         .when(col("affiliate_cnt") >= 2, lit(5))
         .otherwise(lit(2)))
df = df.withColumn("relationship_score", rel_s.cast("double"))

# ── 성장 점수 (max 15) ────────────────────────────────────────────────────────
# 실 파이프라인에서는 ai_feature_table의 asset_growth_90d, invest_growth_90d,
# spend_growth_90d 사용. 현재는 proxy 0.0 적용
df = df.withColumn("asset_growth_90d",  F.rand(seed=51) * 0.30 - 0.05)
df = df.withColumn("invest_growth_90d", F.rand(seed=52) * 0.25 - 0.05)
df = df.withColumn("spend_growth_90d",  F.rand(seed=53) * 0.20 - 0.05)

growth_s = (
    when((col("asset_growth_90d") >= 0.20) & (col("invest_growth_90d") >= 0.15), lit(15))
    .when(col("asset_growth_90d") >= 0.10, lit(10))
    .when(col("spend_growth_90d") >= 0.08, lit(6))
    .otherwise(lit(2))
)
df = df.withColumn("growth_score", growth_s.cast("double"))

# ── 투자 점수 (max 10) ────────────────────────────────────────────────────────
df = df.withColumn("invest_ratio",
    when(col("total_asset") > 0,
         col("invest_total") / col("total_asset")).otherwise(lit(0.0)))

invest_s = (when(col("invest_ratio") >= 0.50, lit(10))
            .when(col("invest_ratio") >= 0.30, lit(7))
            .when(col("invest_total") > 0,     lit(5))
            .otherwise(lit(1)))
df = df.withColumn("investment_score", invest_s.cast("double"))

# ── 라이프스타일 점수 (max 5) ─────────────────────────────────────────────────
lifestyle_s = (
    when((col("avg_steps") >= 10000) & (col("health_score") >= 80), lit(5))
    .when(col("health_score") >= 70, lit(3))
    .otherwise(lit(1))
)
df = df.withColumn("lifestyle_score", lifestyle_s.cast("double"))

# ── 디지털 보너스 (max 5) ─────────────────────────────────────────────────────
# 실 파이프라인에서는 login_cnt_30d, recommend_click_rate 사용. 현재는 proxy 0.0
df = df.withColumn("login_cnt_30d",        (F.rand(seed=54) * 20).cast("double"))
df = df.withColumn("recommend_click_rate",
    when(F.rand(seed=55) > 0.50, F.rand(seed=56) * 0.40).otherwise(lit(0.0)))

digital_s = (
    when((col("login_cnt_30d") >= 15) & (col("recommend_click_rate") >= 0.20), lit(5))
    .when(col("login_cnt_30d") >= 8, lit(3))
    .otherwise(lit(0))
)
df = df.withColumn("digital_bonus", digital_s.cast("double"))

# ── 리스크 차감 (max 20) ──────────────────────────────────────────────────────
# 실 파이프라인에서는 card_drop_ratio, insurance_drop_ratio 사용. 현재는 proxy 0.0
df = df.withColumn("card_drop_ratio",
    when(F.rand(seed=57) > 0.90, F.rand(seed=58) * 0.80).otherwise(lit(0.0)))
df = df.withColumn("insurance_drop_ratio",
    when(F.rand(seed=59) > 0.92, F.rand(seed=60) * 0.80).otherwise(lit(0.0)))

card_risk_s = (when(col("card_drop_ratio") >= 0.80, lit(15))
               .when(col("card_drop_ratio") >= 0.60, lit(10))
               .when(col("card_drop_ratio") >= 0.40, lit(5))
               .otherwise(lit(0)))
ins_risk_s = when(col("insurance_drop_ratio") >= 0.80, lit(5)).otherwise(lit(0))
df = df.withColumn("risk_penalty",
    least(lit(20.0), (card_risk_s + ins_risk_s).cast("double")))

# ── vip_score_final ───────────────────────────────────────────────────────────
df = df.withColumn(
    "vip_score_final",
    greatest(lit(0.0), least(lit(100.0),
        col("asset_score") + col("revenue_score") + col("relationship_score")
        + col("growth_score") + col("investment_score") + col("lifestyle_score")
        + col("digital_bonus") - col("risk_penalty")
    ))
)

# ── vip_level ─────────────────────────────────────────────────────────────────
df = df.withColumn(
    "vip_level",
    when(col("vip_score_final") >= 90, lit("VIP_CONFIRMED"))
    .when(col("vip_score_final") >= 80, lit("PB_TARGET"))
    .when(col("vip_score_final") >= 70, lit("POTENTIAL_VIP"))
    .when(col("vip_score_final") >= 60, lit("GROWTH_CUSTOMER"))
    .otherwise(lit("GENERAL"))
)

# ── recommended_action ────────────────────────────────────────────────────────
df = df.withColumn(
    "recommended_action",
    when(col("vip_level") == "VIP_CONFIRMED",   lit("PB_CALL_NOW"))
    .when(col("vip_level") == "PB_TARGET",      lit("PB_MEETING"))
    .when(col("vip_level") == "POTENTIAL_VIP",  lit("PREMIUM_CAMPAIGN"))
    .when(col("vip_level") == "GROWTH_CUSTOMER", lit("WEALTH_NURTURING"))
    .otherwise(lit(None))
)

# ── priority_rank: vip_score_final 전체 내림차순 ──────────────────────────────
window_spec = Window.orderBy(col("vip_score_final").desc())
df = df.withColumn("priority_rank", F.rank().over(window_spec))

vip_mart = df.select(
    col("global_id"),
    col("total_asset"),
    col("lifesync_score"),
    col("vip_score_final"),
    col("vip_level"),
    col("priority_rank"),
    col("recommended_action"),
    col("asset_score"),
    col("revenue_score"),
    col("relationship_score"),
    col("growth_score"),
    col("investment_score"),
    col("lifestyle_score"),
    col("digital_bonus"),
    col("risk_penalty"),
    col("customer_grade"),
    col("health_score"),
    col("pb_score"),
    col("churn_score"),
    lit(date_formatted).alias("dt"),
)

output_path = f"s3://{S3_CURATED_BUCKET}/vip_mart/"
print(f"[vip_mart] Writing output to {output_path}")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
vip_mart.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[vip_mart] Job completed successfully. Output: {output_path}")
spark.stop()
