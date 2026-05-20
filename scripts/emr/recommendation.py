import os
import sys
from datetime import datetime, timedelta
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
valid_until = (datetime.strptime(date_formatted, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")

spark = SparkSession.builder \
    .appName("recommendation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[recommendation] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

customer360_path = f"s3://{S3_CURATED_BUCKET}/customer_360_profile/dt={date_formatted}/"
score_mart_path  = f"s3://{S3_CURATED_BUCKET}/score_mart/dt={date_formatted}/"

print(f"[recommendation] Reading customer_360_profile from {customer360_path}")
df_c360 = spark.read.parquet(customer360_path).select(
    "global_id", "income_grade", "invest_total",
    "insurance_premium", "health_score", "wearable_flag",
)

print(f"[recommendation] Reading score_mart from {score_mart_path}")
df_score = spark.read.parquet(score_mart_path).select(
    "global_id", "customer_grade", "pb_score", "churn_score",
)

df = df_c360.join(df_score, on="global_id", how="left")
df = df.fillna({"pb_score": 10.0, "churn_score": 0.0, "customer_grade": "CARE"})

# ── 추천 후보 정의 (recommendation_code, name, score, reason_1, reason_2, condition) ─────
REC_CANDIDATES = [
    {
        "code":     "PB_CENTER",
        "name":     "PB 프라이빗 뱅킹",
        "score":    95.0,
        "reason_1": "VIP 고객 전용 자산관리",
        "reason_2": "전문 PB 1:1 서비스",
        "condition": col("customer_grade").isin("VIP", "GOLD") | (col("pb_score") >= 70),
    },
    {
        "code":     "ETF_PRODUCT",
        "name":     "ETF 투자 상품",
        "score":    80.0,
        "reason_1": "투자 포트폴리오 미보유",
        "reason_2": "분산투자 포트폴리오 추천",
        "condition": col("invest_total") == 0,
    },
    {
        "code":     "PREMIUM_CARD",
        "name":     "프리미엄 신용카드",
        "score":    75.0,
        "reason_1": "고소득 고객 혜택 최적화",
        "reason_2": "카드 포인트 및 할인 혜택",
        "condition": col("income_grade") == "HIGH",
    },
    {
        "code":     "HEALTH_CHECKUP",
        "name":     "건강검진 패키지",
        "score":    70.0,
        "reason_1": "건강 위험 지수 높음",
        "reason_2": "예방적 건강관리 서비스",
        "condition": col("health_score") < 60,
    },
    {
        "code":     "INSURANCE_PRODUCT",
        "name":     "생명보험 상품",
        "score":    65.0,
        "reason_1": "보험 미가입 고객",
        "reason_2": "리스크 헤지 필요",
        "condition": col("insurance_premium") == 0,
    },
    {
        "code":     "RETENTION_COUPON",
        "name":     "멤버십 리텐션 쿠폰",
        "score":    60.0,
        "reason_1": "이탈 위험 고객",
        "reason_2": "재참여 유도 혜택",
        "condition": col("churn_score") >= 50,
    },
    {
        "code":     "BASIC_SERVICE",
        "name":     "기본 금융 서비스",
        "score":    50.0,
        "reason_1": "신규 가입 고객",
        "reason_2": "lifesync 서비스 소개",
        "condition": lit(True),
    },
]

# ── 조건별 후보 DataFrame 생성 후 UNION ─────────────────────────────────────────
print("[recommendation] Building recommendation candidates")

candidates = None
for rec in REC_CANDIDATES:
    cand = df.filter(rec["condition"]).select(
        col("global_id"),
        lit(rec["code"]).alias("recommendation_code"),
        lit(rec["name"]).alias("recommendation_name"),
        lit(rec["score"]).alias("recommendation_score"),
        lit(rec["reason_1"]).alias("reason_1"),
        lit(rec["reason_2"]).alias("reason_2"),
    )
    candidates = cand if candidates is None else candidates.union(cand)

# ── 고객별 recommendation_score 내림차순으로 rec_rank 부여 (상위 3개만) ────────
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
