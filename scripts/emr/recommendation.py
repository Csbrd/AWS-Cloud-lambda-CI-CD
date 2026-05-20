import os
import sys
from datetime import datetime, timedelta
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
valid_until = (datetime.strptime(date_formatted, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")

spark = SparkSession.builder \
    .appName("recommendation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[recommendation] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

# ── 데이터 읽기 ───────────────────────────────────────────────────────────────
customer360_path = f"s3://{S3_CURATED_BUCKET}/customer_360_profile/dt={date_formatted}/"
score_mart_path  = f"s3://{S3_CURATED_BUCKET}/score_mart/dt={date_formatted}/"
health_mart_path = f"s3://{S3_CURATED_BUCKET}/health_mart/dt={date_formatted}/"

print(f"[recommendation] Reading customer_360_profile from {customer360_path}")
df_c360 = spark.read.parquet(customer360_path).select(
    "global_id",
    "latest_balance", "invest_total",
    "insurance_premium", "online_insurance_premium",
    "card_total_spend", "invest_product_count",
    "bank_tx_count", "card_tx_count",
    "insurance_count", "online_insurance_count",
    "wearable_flag",
)

print(f"[recommendation] Reading score_mart from {score_mart_path}")
df_score = spark.read.parquet(score_mart_path).select(
    "global_id", "customer_grade", "pb_score", "churn_score", "health_score",
)

print(f"[recommendation] Reading health_mart from {health_mart_path}")
df_hmart = spark.read.parquet(health_mart_path).select(
    "global_id", "health_risk", "hospital_visit_cnt", "stress",
)

# ── 조인 및 fillna ────────────────────────────────────────────────────────────
print("[recommendation] Joining datasets")
df = df_c360 \
    .join(df_score, on="global_id", how="left") \
    .join(df_hmart,  on="global_id", how="left")

df = df.fillna({
    "latest_balance": 0.0, "invest_total": 0.0,
    "insurance_premium": 0.0, "online_insurance_premium": 0.0,
    "card_total_spend": 0.0, "invest_product_count": 0,
    "bank_tx_count": 0, "card_tx_count": 0,
    "insurance_count": 0, "online_insurance_count": 0,
    "wearable_flag": "N",
    "customer_grade": "CARE", "pb_score": 10.0,
    "churn_score": 0.0, "health_score": 50.0,
    "health_risk": 50.0, "hospital_visit_cnt": 0, "stress": 40.0,
})

# ── 공통 지표 계산 ─────────────────────────────────────────────────────────────
df = df.withColumn("premium_total",
    col("insurance_premium") + col("online_insurance_premium"))
df = df.withColumn("total_asset",
    col("latest_balance") + col("invest_total") + col("premium_total"))

affiliate_expr = (
    when(col("bank_tx_count")          > 0, lit(1)).otherwise(lit(0)) +
    when(col("card_tx_count")          > 0, lit(1)).otherwise(lit(0)) +
    when(col("invest_product_count")   > 0, lit(1)).otherwise(lit(0)) +
    when(col("insurance_count")        > 0, lit(1)).otherwise(lit(0)) +
    when(col("online_insurance_count") > 0, lit(1)).otherwise(lit(0))
)
df = df.withColumn("affiliate_cnt", affiliate_expr)

# ── 등급 가중치 (grade_weight) ────────────────────────────────────────────────
grade_w = (
    when(col("customer_grade") == "VIP",    lit(20.0))
    .when(col("customer_grade") == "GOLD",   lit(15.0))
    .when(col("customer_grade") == "SILVER",  lit(8.0))
    .when(col("customer_grade") == "BASIC",   lit(3.0))
    .otherwise(lit(0.0))
)
df = df.withColumn("grade_weight", grade_w)

# ── 디지털 반응도 (실 파이프라인: login_cnt_30d, push_click_rate, product_click. 현재 proxy 0.0) ──
df = df.withColumn("login_cnt_30d",      lit(0.0))
df = df.withColumn("push_click_rate",    lit(0.0))
df = df.withColumn("product_click_flag", lit(0.0))
df = df.withColumn("digital_score",
    when(col("login_cnt_30d") >= 15,       lit(10.0)).otherwise(lit(0.0))
    + when(col("push_click_rate") >= 0.30, lit(10.0)).otherwise(lit(0.0))
    + when(col("product_click_flag") >= 1, lit(15.0)).otherwise(lit(0.0))
)

# ── 리스크 차감 (실 파이프라인: delinquency, complaint, inactive_days. 현재 proxy 0.0) ──
df = df.withColumn("delinquency_flag", lit(0.0))
df = df.withColumn("complaint_flag",   lit(0.0))
df = df.withColumn("inactive_days",    lit(0.0))
df = df.withColumn("risk_penalty",
    least(lit(50.0),
        when(col("delinquency_flag") >= 1,  lit(30.0)).otherwise(lit(0.0))
        + when(col("complaint_flag")   >= 1, lit(20.0)).otherwise(lit(0.0))
        + when(col("inactive_days")    >= 30, lit(10.0)).otherwise(lit(0.0))
    )
)

# ── 상품별 적합도 점수 계산 ────────────────────────────────────────────────────
print("[recommendation] Calculating product scores")

# PB_CENTER: 총자산 + VIP/GOLD 여부 + 다채널 거래
df = df.withColumn("pb_product_score",
    when(col("total_asset") >= 500_000_000, lit(40.0))
    .when(col("total_asset") >= 100_000_000, lit(30.0))
    .otherwise(lit(0.0))
    + when(col("customer_grade").isin("VIP", "GOLD"), lit(20.0)).otherwise(lit(0.0))
    + when(col("affiliate_cnt") >= 3, lit(10.0)).otherwise(lit(0.0))
)

# ETF_PRODUCT: 투자 자산 + ETF 경험 + 잔액 충분 + 위험 고객 차감
df = df.withColumn("etf_product_score",
    when(col("invest_total") >= 50_000_000,  lit(30.0)).otherwise(lit(0.0))
    + when(col("invest_product_count") > 0,  lit(20.0)).otherwise(lit(0.0))
    + when(col("latest_balance") >= 30_000_000, lit(20.0)).otherwise(lit(0.0))
    + when(col("customer_grade").isin("VIP", "GOLD"), lit(10.0)).otherwise(lit(0.0))
    - when(col("churn_score") >= 70, lit(20.0)).otherwise(lit(0.0))
)

# PREMIUM_CARD: 카드 소비 + VIP/GOLD + 카드 미보유
# 해외결제 데이터 없음 (proxy 0)
df = df.withColumn("card_product_score",
    when(col("card_total_spend") >= 3_000_000, lit(40.0)).otherwise(lit(0.0))
    + when(col("customer_grade").isin("VIP", "GOLD"), lit(20.0)).otherwise(lit(0.0))
    + when(col("card_tx_count") == 0, lit(10.0)).otherwise(lit(0.0))
)

# HEALTH_CHECKUP: 건강위험도 + 병원방문 + 스트레스 + 웨어러블 보유
df = df.withColumn("health_product_score",
    when(col("health_risk") >= 70, lit(40.0)).otherwise(lit(0.0))
    + when(col("hospital_visit_cnt") >= 3, lit(20.0)).otherwise(lit(0.0))
    + when(col("stress") >= 60, lit(20.0)).otherwise(lit(0.0))
    + when(col("wearable_flag") == "Y", lit(10.0)).otherwise(lit(0.0))
)

# INSURANCE_PRODUCT: 보험 미가입 + 병원방문 이력 + VIP/GOLD
# 자녀 여부 데이터 없음 (proxy 0)
df = df.withColumn("ins_product_score",
    when(
        (col("insurance_premium") == 0) & (col("online_insurance_premium") == 0),
        lit(40.0)
    ).otherwise(lit(0.0))
    + when(col("hospital_visit_cnt") > 0, lit(20.0)).otherwise(lit(0.0))
    + when(col("customer_grade").isin("VIP", "GOLD"), lit(10.0)).otherwise(lit(0.0))
)

# RETENTION_COUPON: 이탈 위험 + 장기 미접속
# 소비급감/앱활동감소 데이터 없음 (proxy 0)
df = df.withColumn("ret_product_score",
    when(col("churn_score") >= 70, lit(40.0))
    .when(col("churn_score") >= 50, lit(20.0))
    .otherwise(lit(0.0))
    + when(col("inactive_days") >= 30, lit(25.0)).otherwise(lit(0.0))
)

# ── 추천 후보 생성: candidate_score = product_score + grade_weight + digital - risk ──
# 각 상품별 candidate_score가 threshold 이상인 고객만 후보로 포함
print("[recommendation] Building recommendation candidates")

PRODUCTS = [
    ("PB_CENTER",         "PB 프라이빗 뱅킹",  "pb_product_score",     90,
     "VIP 고객 전용 자산관리",    "전문 PB 1:1 서비스"),
    ("ETF_PRODUCT",       "ETF 투자 상품",      "etf_product_score",    70,
     "투자 포트폴리오 추천",      "분산투자 포트폴리오"),
    ("PREMIUM_CARD",      "프리미엄 신용카드",  "card_product_score",   60,
     "고소득 고객 혜택 최적화",   "카드 포인트 및 할인 혜택"),
    ("HEALTH_CHECKUP",    "건강검진 패키지",    "health_product_score", 50,
     "건강 위험 지수 높음",       "예방적 건강관리 서비스"),
    ("INSURANCE_PRODUCT", "생명보험 상품",      "ins_product_score",    50,
     "보험 미가입 고객",          "리스크 헤지 필요"),
    ("RETENTION_COUPON",  "멤버십 리텐션 쿠폰", "ret_product_score",    60,
     "이탈 위험 고객",            "재참여 유도 혜택"),
]

candidates = None
for code, name, score_col, threshold, r1, r2 in PRODUCTS:
    candidate_score_expr = greatest(lit(0.0), least(lit(100.0),
        col(score_col) + col("grade_weight") + col("digital_score") - col("risk_penalty")
    ))
    cand = (
        df.withColumn("candidate_score", candidate_score_expr)
          .filter(col("candidate_score") >= threshold)
          .select(
              col("global_id"),
              lit(code).alias("recommendation_code"),
              lit(name).alias("recommendation_name"),
              col("candidate_score").alias("recommendation_score"),
              lit(r1).alias("reason_1"),
              lit(r2).alias("reason_2"),
          )
    )
    candidates = cand if candidates is None else candidates.union(cand)

# ── 고객별 recommendation_score 내림차순으로 rec_rank 부여 (상위 3개만) ─────────
print("[recommendation] Ranking recommendations per customer")
window_spec = Window.partitionBy("global_id").orderBy(col("recommendation_score").desc())
candidates = candidates.withColumn("rec_rank", F.rank().over(window_spec))

recommendation = candidates.filter(col("rec_rank") <= 3).select(
    col("global_id"),
    col("rec_rank"),
    col("recommendation_code"),
    col("recommendation_name"),
    col("recommendation_score"),
    col("reason_1"),
    col("reason_2"),
    lit(valid_until).alias("valid_until"),
    lit(date_formatted).alias("process_dt"),
    lit(date_formatted).alias("dt"),
)

output_path = f"s3://{S3_CURATED_BUCKET}/recommendation_mart/"
print(f"[recommendation] Writing output to {output_path}")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
recommendation.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[recommendation] Job completed successfully. Output: {output_path}")
spark.stop()
