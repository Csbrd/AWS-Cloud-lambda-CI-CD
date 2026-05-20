import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, least, greatest

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
S3_CURATED_BUCKET = os.environ.get("S3_CURATED_BUCKET", "lifesync-curated")

date_formatted = f"{BATCH_DATE[:4]}-{BATCH_DATE[4:6]}-{BATCH_DATE[6:8]}"

spark = SparkSession.builder \
    .appName("score_mart") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[score_mart] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

customer360_path = f"s3://{S3_CURATED_BUCKET}/customer_360_profile/dt={date_formatted}/"
print(f"[score_mart] Reading customer_360_profile from {customer360_path}")
df = spark.read.parquet(customer360_path)

print("[score_mart] Calculating LifeSync Score components (설계 문서 기준)")

# ── financial_score (max 40) ───────────────────────────────────────────────────
# 1) balance_score (max 15)
balance_s = (when(col("latest_balance") >= 50_000_000, lit(15))
             .when(col("latest_balance") >= 30_000_000, lit(12))
             .when(col("latest_balance") >= 10_000_000, lit(8))
             .when(col("latest_balance") >= 3_000_000,  lit(5))
             .otherwise(lit(2)))
df = df.withColumn("balance_score", balance_s.cast("double"))

# 2) card_score (max 10)
card_s = (when(col("card_total_spend") >= 5_000_000, lit(10))
          .when(col("card_total_spend") >= 3_000_000, lit(8))
          .when(col("card_total_spend") >= 1_500_000, lit(5))
          .when(col("card_total_spend") >= 500_000,   lit(3))
          .otherwise(lit(1)))
df = df.withColumn("card_score", card_s.cast("double"))

# 3) invest_score (max 15)
invest_s = (when(col("invest_total") >= 100_000_000, lit(15))
            .when(col("invest_total") >= 50_000_000,  lit(12))
            .when(col("invest_total") >= 30_000_000,  lit(8))
            .when(col("invest_total") >= 10_000_000,  lit(5))
            .otherwise(lit(1)))
df = df.withColumn("invest_score", invest_s.cast("double"))

df = df.withColumn("financial_score",
    col("balance_score") + col("card_score") + col("invest_score"))

# ── health_sub_score (max 25) ─────────────────────────────────────────────────
# steps_score (max 10): avg_steps 구간별 점수
steps_s = (when(col("avg_steps") >= 12000, lit(10))
           .when(col("avg_steps") >= 10000, lit(8))
           .when(col("avg_steps") >= 7000,  lit(6))
           .when(col("avg_steps") >= 5000,  lit(3))
           .otherwise(lit(1)))
df = df.withColumn("steps_score", steps_s.cast("double"))

# wellness_score (max 10): healthcare.health_score (0~100) → 구간별 점수
wellness_s = (when(col("health_score") >= 90, lit(10))
              .when(col("health_score") >= 80, lit(8))
              .when(col("health_score") >= 70, lit(6))
              .when(col("health_score") >= 60, lit(3))
              .otherwise(lit(1)))
df = df.withColumn("wellness_score", wellness_s.cast("double"))

# stress_sleep_score (max 5): avg_stress(wearable) + sleep_score(healthcare) 조합
# 스트레스 낮음(<=30) + 수면 양호(>=70) → 5
# 스트레스 높음(>60)  + 수면 낮음(<40)  → 1
# 그 외 (보통)                           → 3
stress_sleep_s = (
    when((col("avg_stress") <= 30) & (col("sleep_score") >= 70), lit(5))
    .when((col("avg_stress") > 60)  & (col("sleep_score") < 40),  lit(1))
    .otherwise(lit(3))
)
df = df.withColumn("stress_sleep_score", stress_sleep_s.cast("double"))

df = df.withColumn("health_sub_score",
    col("steps_score") + col("wellness_score") + col("stress_sleep_score"))

# ── relationship_score (max 15) ───────────────────────────────────────────────
# 이용 계열사 수 (은행/카드/증권/보험/온보험/병원)
affiliate_cnt_expr = (
    when(col("bank_tx_count")          > 0, lit(1)).otherwise(lit(0)) +
    when(col("card_tx_count")          > 0, lit(1)).otherwise(lit(0)) +
    when(col("invest_product_count")   > 0, lit(1)).otherwise(lit(0)) +
    when(col("insurance_count")        > 0, lit(1)).otherwise(lit(0)) +
    when(col("online_insurance_count") > 0, lit(1)).otherwise(lit(0)) +
    when(col("hospital_visit_count")   > 0, lit(1)).otherwise(lit(0))
)
df = df.withColumn("affiliate_cnt", affiliate_cnt_expr)

affiliate_s = (when(col("affiliate_cnt") >= 5, lit(10))
               .when(col("affiliate_cnt") >= 3, lit(7))
               .when(col("affiliate_cnt") >= 2, lit(4))
               .otherwise(lit(2)))
df = df.withColumn("affiliate_score", affiliate_s.cast("double"))

# consent_score (max 5): consent_ratio from S3 consent snapshot
consent_s = (when(col("consent_ratio") >= 0.8, lit(5))
             .when(col("consent_ratio") >= 0.5, lit(3))
             .otherwise(lit(0)))
df = df.withColumn("consent_score", consent_s.cast("double"))

df = df.withColumn("relationship_score",
    col("affiliate_score") + col("consent_score"))

# ── growth_score (max 10) ─────────────────────────────────────────────────────
# 최근 3개월 증가 추세 proxy: 투자/건강/소비 보유 여부로 추정
# 실 파이프라인에서는 90d 증가율(invest_growth_90d, spend_growth_90d 등) 사용
df = df.withColumn(
    "growth_score",
    when(
        (col("invest_total") > 0) & (col("health_score") >= 70), lit(10.0)
    ).when(
        (col("card_total_spend") > 0) & (col("invest_total") > 0), lit(7.0)
    ).when(
        (col("invest_total") > 0) | (col("card_total_spend") > 0), lit(4.0)
    ).otherwise(lit(1.0))
)

# ── risk_score (최대 20 차감) ─────────────────────────────────────────────────
# 1) 건강 리스크 (max 15)
visit_risk  = when(col("hospital_visit_count") > 6, lit(5.0)).otherwise(lit(0.0))
health_risk = when(col("health_score") < 40, lit(10.0)).otherwise(lit(0.0))
df = df.withColumn("health_risk", visit_risk + health_risk)

# 2) 금융 리스크 (max 15): 잔액급감/카드연체 데이터 없어 0 적용
# 실 파이프라인에서는 asset_drop_ratio, card_drop_ratio 사용
df = df.withColumn("financial_risk", lit(0.0))

df = df.withColumn("risk_score",
    least(lit(20.0), col("health_risk") + col("financial_risk")))

# ── lifesync_score (base 없이 합산, 설계 문서 기준) ───────────────────────────
# = financial + health + relationship + growth - risk  (max 100, min 0)
df = df.withColumn(
    "raw_score",
    col("financial_score")
    + col("health_sub_score")
    + col("relationship_score")
    + col("growth_score")
    - col("risk_score")
)

df = df.withColumn(
    "lifesync_score",
    greatest(lit(0.0), least(lit(100.0), col("raw_score")))
)

print("[score_mart] Assigning customer_grade")
df = df.withColumn(
    "customer_grade",
    when(col("lifesync_score") >= 90, lit("VIP"))
    .when(col("lifesync_score") >= 80, lit("GOLD"))
    .when(col("lifesync_score") >= 70, lit("SILVER"))
    .when(col("lifesync_score") >= 60, lit("BASIC"))
    .otherwise(lit("CARE"))
)

# vip_score = lifesync_score
df = df.withColumn("vip_score", col("lifesync_score"))

# pb_score: 총자산 기반 PB 적합도 점수
total_asset_expr = col("latest_balance") + col("invest_total")
pb_s = (when(total_asset_expr >= 500_000_000, lit(100.0))
        .when(total_asset_expr >= 300_000_000, lit(85.0))
        .when(total_asset_expr >= 100_000_000, lit(70.0))
        .when(total_asset_expr >= 50_000_000,  lit(50.0))
        .when(total_asset_expr >= 10_000_000,  lit(30.0))
        .otherwise(lit(10.0)))
df = df.withColumn("pb_score", pb_s)

# churn_score: risk_score 기반 이탈 위험 점수 (0~100 정규화)
df = df.withColumn("churn_score", F.round(col("risk_score") / lit(20.0) * lit(100.0), 1))

score_mart = df.select(
    col("global_id"),
    col("lifesync_score"),
    col("vip_score"),
    col("pb_score"),
    F.coalesce(col("health_score"), lit(50.0)).cast("double").alias("health_score"),
    col("churn_score"),
    col("customer_grade"),
    lit(date_formatted).alias("score_dt"),
    col("dt"),
)

output_path = f"s3://{S3_CURATED_BUCKET}/score_mart/"
print(f"[score_mart] Writing output to {output_path}")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
score_mart.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[score_mart] Job completed successfully. Output: {output_path}")
spark.stop()
