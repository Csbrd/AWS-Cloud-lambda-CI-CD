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
S3_CURATED_BUCKET   = os.environ.get("S3_CURATED_BUCKET",   "lifesync-curated")

date_formatted = f"{BATCH_DATE[:4]}-{BATCH_DATE[4:6]}-{BATCH_DATE[6:8]}"

spark = SparkSession.builder \
    .appName("health_mart") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[health_mart] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

def read_processed(subsidiary):
    path = f"s3://{S3_PROCESSED_BUCKET}/{subsidiary}/dt={date_formatted}/"
    print(f"[health_mart] Reading {subsidiary} from {path}")
    return spark.read.parquet(path)

df_healthcare = read_processed("healthcare")
df_hospital   = read_processed("hospital")
df_wearable   = read_processed("wearable")

print("[health_mart] Aggregating healthcare data")
healthcare_agg = df_healthcare.groupBy("global_id").agg(
    F.avg("diet_score").alias("diet_score"),
    F.avg("sleep_score").alias("sleep_score"),
    F.avg("calories").alias("calories"),
    F.avg("bmi").alias("bmi"),
)

print("[health_mart] Aggregating hospital data")
hospital_agg = df_hospital.groupBy("global_id").agg(
    F.count("*").alias("hospital_visit_count"),
)

print("[health_mart] Aggregating wearable data")
wearable_agg = df_wearable.groupBy("global_id").agg(
    F.avg("heart_rate").alias("avg_heart_rate"),
    F.avg("steps").alias("avg_steps"),
    F.avg("stress_score").alias("avg_stress"),
    F.avg("spo2_pct").alias("spo2"),
    F.max("record_date").alias("last_sync_date"),
)

print("[health_mart] Building base global_id list from UNION of health-related processed data")
base_ids = (
    df_healthcare.select("global_id")
    .union(df_hospital.select("global_id"))
    .union(df_wearable.select("global_id"))
    .distinct()
)

print("[health_mart] Joining all datasets on global_id")
df = base_ids \
    .join(healthcare_agg, on="global_id", how="left") \
    .join(hospital_agg,   on="global_id", how="left") \
    .join(wearable_agg,   on="global_id", how="left")

# healthcare 데이터 존재 여부 (fillna 전에 판단)
df = df.withColumn("has_healthcare", when(col("bmi").isNotNull(), lit(1)).otherwise(lit(0)))

df = df.fillna({
    "hospital_visit_count": 0,
    "avg_heart_rate":       75.0,
    "avg_steps":            5000.0,
    "avg_stress":           40.0,
    "diet_score":           50.0,
    "sleep_score":          50.0,
    "calories":             0.0,
    "bmi":                  22.0,
    "spo2":                 98.0,
})

# ── Health Score 공식 (설계 문서 기준) ─────────────────────────────────────────

print("[health_mart] Computing health_score (설계 문서 공식)")

# 1) activity_score (max 25)
#    steps_score: avg_steps 구간
#    workout_score: avg_steps 기반 주간 운동일수 프록시 (직접 데이터 없음)
steps_s   = (when(col("avg_steps") >= 12000, lit(15))
             .when(col("avg_steps") >= 10000, lit(12))
             .when(col("avg_steps") >= 7000,  lit(8))
             .when(col("avg_steps") >= 5000,  lit(5))
             .otherwise(lit(2)))
workout_s = (when(col("avg_steps") >= 10000, lit(10))
             .when(col("avg_steps") >= 7000,  lit(7))
             .when(col("avg_steps") >= 5000,  lit(4))
             .otherwise(lit(1)))
df = df.withColumn("activity_score", steps_s + workout_s)

# 2) bio_score (max 25)
#    hr_score + spo2_score(기본값 8, 데이터 없음) + bmi_score
hr_s  = (when(col("avg_heart_rate") <= 75,  lit(10))
         .when(col("avg_heart_rate") <= 90,  lit(8))
         .when(col("avg_heart_rate") <= 110, lit(5))
         .otherwise(lit(2)))
spo2_s = (when(col("spo2") >= 98, lit(10))
          .when(col("spo2") >= 95, lit(8))
          .when(col("spo2") >= 90, lit(5))
          .otherwise(lit(2)))
bmi_s  = (when((col("bmi") >= 18.5) & (col("bmi") <= 24.9), lit(5))
          .when(col("bmi") <= 29.9, lit(3))
          .otherwise(lit(1)))
df = df.withColumn("bio_score", hr_s + spo2_s + bmi_s)

# 3) lifestyle_score (max 15)
stress_s = (when(col("avg_stress") <= 30, lit(8))
            .when(col("avg_stress") <= 60, lit(5))
            .otherwise(lit(2)))
sleep_s  = (when(col("sleep_score") >= 80, lit(7))
            .when(col("sleep_score") >= 60, lit(5))
            .when(col("sleep_score") >= 40, lit(3))
            .otherwise(lit(1)))
df = df.withColumn("lifestyle_score", stress_s + sleep_s)

# 4) prevent_score (max 15)
checkup_s   = when(col("has_healthcare") == 1, lit(10)).otherwise(lit(0))
habit_bonus = when(col("avg_steps") >= 10000, lit(5)).otherwise(lit(0))
df = df.withColumn("prevent_score", checkup_s + habit_bonus)

# 5) disease_penalty (max 15)
#    base_risk = min(100, visit_count * 10)
#    만성질환(E11/I10) 있으면 base_risk 최소 80으로 상향
base_risk_expr = least(
    lit(100),
    (col("hospital_visit_count").cast("double") * lit(10.0)).cast("int"),
)
disease_base_p = (when(base_risk_expr >= 80, lit(15))
                  .when(base_risk_expr >= 60, lit(10))
                  .when(base_risk_expr >= 40, lit(5))
                  .otherwise(lit(0)))
df = df.withColumn("disease_penalty", least(lit(15), disease_base_p))

# 6) visit_penalty (max 10)
visit_p = (when(col("hospital_visit_count") >= 6, lit(10))
           .when(col("hospital_visit_count") >= 4, lit(7))
           .when(col("hospital_visit_count") >= 2, lit(3))
           .otherwise(lit(0)))
df = df.withColumn("visit_penalty", visit_p)

# 7) health_score_raw (10-80) → 정규화 (13-100)
raw_expr = (col("activity_score") + col("bio_score") + col("lifestyle_score")
            + col("prevent_score") - col("disease_penalty") - col("visit_penalty"))
health_score_raw = least(lit(80), greatest(lit(10), raw_expr))
df = df.withColumn(
    "health_score",
    F.round(health_score_raw.cast("double") / lit(80.0) * lit(100.0)).cast("int"),
)

# ── 5단계 등급 (설계 문서 기준) ───────────────────────────────────────────────
df = df.withColumn(
    "health_grade",
    when(col("health_score") >= 90, lit("EXCELLENT"))
    .when(col("health_score") >= 80, lit("GOOD"))
    .when(col("health_score") >= 65, lit("NORMAL"))
    .when(col("health_score") >= 50, lit("WARNING"))
    .otherwise(lit("RISK"))
)

# ── BMI 카테고리 ──────────────────────────────────────────────────────────────
df = df.withColumn(
    "bmi_category",
    when(col("bmi") < 18.5, lit("UNDERWEIGHT"))
    .when(col("bmi") < 25.0, lit("NORMAL"))
    .when(col("bmi") < 30.0, lit("OVERWEIGHT"))
    .otherwise(lit("OBESE"))
)

df = df.withColumn("health_risk", (lit(100.0) - col("health_score")).cast("double"))

df = df.withColumn(
    "next_health_action",
    when(col("health_grade") == "RISK",    lit("EMERGENCY_CARE"))
    .when(col("health_grade") == "WARNING", lit("HEALTH_CHECKUP"))
    .when(col("avg_steps") < 5000,          lit("EXERCISE_PROGRAM"))
    .otherwise(lit("HEALTH_MAINTENANCE"))
)

df = df.withColumn("dt", lit(date_formatted))

health_mart = df.select(
    col("global_id"),
    col("avg_steps"),
    col("avg_heart_rate").alias("avg_hr"),
    col("spo2"),
    col("bmi"),
    col("avg_stress").alias("stress"),
    col("sleep_score").alias("sleep_avg"),
    col("hospital_visit_count").alias("hospital_visit_cnt"),
    col("health_risk"),
    col("health_score"),
    col("health_grade"),
    col("next_health_action"),
    col("dt"),
)

output_path = f"s3://{S3_CURATED_BUCKET}/health_mart/"
print(f"[health_mart] Writing output to {output_path}")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
health_mart.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[health_mart] Job completed successfully. Output: {output_path}")
spark.stop()
