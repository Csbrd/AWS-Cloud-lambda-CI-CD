import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, coalesce, when

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
S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "lifesync-raw")

date_formatted = f"{BATCH_DATE[:4]}-{BATCH_DATE[4:6]}-{BATCH_DATE[6:8]}"

spark = SparkSession.builder \
    .appName("customer360") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[customer360] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

def read_processed(subsidiary):
    path = f"s3://{S3_PROCESSED_BUCKET}/{subsidiary}/dt={date_formatted}/"
    print(f"[customer360] Reading {subsidiary} from {path}")
    return spark.read.parquet(path)

print("[customer360] Reading all 8 subsidiary datasets")
df_bank             = read_processed("bank")
df_card             = read_processed("card")
df_securities       = read_processed("securities")
df_insurance        = read_processed("insurance")
df_online_insurance = read_processed("online_insurance")
df_healthcare       = read_processed("healthcare")
df_hospital         = read_processed("hospital")
df_wearable         = read_processed("wearable")

print("[customer360] Aggregating bank data")
bank_agg = df_bank.groupBy("global_id").agg(
    F.count("*").alias("bank_tx_count"),
    F.sum("tx_amount").alias("bank_tx_total"),
    F.max("balance").alias("latest_balance"),
    F.avg("tx_amount").alias("bank_avg_tx_amount")
)

print("[customer360] Aggregating card data")
card_agg = df_card.groupBy("global_id").agg(
    F.count("*").alias("card_tx_count"),
    F.sum("spend_amount").alias("card_total_spend"),
    F.countDistinct("merchant_category").alias("card_category_count"),
    F.avg("spend_amount").alias("card_avg_spend")
)

print("[customer360] Aggregating securities data")
securities_agg = df_securities.groupBy("global_id").agg(
    F.sum("invest_amount").alias("invest_total"),
    F.countDistinct("product_type").alias("invest_product_count")
)

print("[customer360] Aggregating insurance data")
insurance_agg = df_insurance.groupBy("global_id").agg(
    F.sum("premium_amount").alias("insurance_premium"),
    F.count("*").alias("insurance_count")
)

print("[customer360] Aggregating online_insurance data")
online_insurance_agg = df_online_insurance.groupBy("global_id").agg(
    F.sum("premium_amount").alias("online_insurance_premium"),
    F.count("*").alias("online_insurance_count")
)

print("[customer360] Aggregating healthcare data")
healthcare_agg = df_healthcare.groupBy("global_id").agg(
    F.avg("diet_score").alias("diet_score"),
    F.avg("sleep_score").alias("sleep_score"),
    F.avg("calories").alias("calories"),
    F.avg("bmi").alias("bmi"),
    ((F.avg("diet_score") + F.avg("sleep_score")) / 2).alias("health_score"),
)

print("[customer360] Aggregating hospital data")
hospital_agg = df_hospital.groupBy("global_id").agg(
    F.count("*").alias("hospital_visit_count"),
)

print("[customer360] Building base global_id list from UNION of all 8 processed subsidiaries")
base_ids = (
    df_bank.select("global_id")
    .union(df_card.select("global_id"))
    .union(df_securities.select("global_id"))
    .union(df_insurance.select("global_id"))
    .union(df_online_insurance.select("global_id"))
    .union(df_healthcare.select("global_id"))
    .union(df_hospital.select("global_id"))
    .union(df_wearable.select("global_id"))
    .distinct()
)

print("[customer360] Aggregating consent data")
consent_df = spark.read.json(f"s3://{S3_RAW_BUCKET}/consent/dt={date_formatted}/")
consent_agg = consent_df.groupBy("global_id").agg(
    (F.sum(when(col("consent_flag") == "Y", lit(1)).otherwise(lit(0))).cast("double")
     / F.count("*")).alias("consent_ratio")
)

print("[customer360] Aggregating wearable data")
wearable_agg = df_wearable.groupBy("global_id").agg(
    F.avg("steps").alias("avg_steps"),
    F.avg("heart_rate").alias("avg_heart_rate"),
    F.avg("stress_score").alias("avg_stress"),
    F.avg("spo2_pct").alias("avg_spo2"),
    F.lit("Y").alias("wearable_flag"),
)

print("[customer360] Joining all datasets on global_id")
base = base_ids
base = base.join(healthcare_agg,        on="global_id", how="left")
base = base.join(bank_agg,              on="global_id", how="left")
base = base.join(card_agg,              on="global_id", how="left")
base = base.join(securities_agg,        on="global_id", how="left")
base = base.join(insurance_agg,         on="global_id", how="left")
base = base.join(online_insurance_agg,  on="global_id", how="left")
base = base.join(hospital_agg,          on="global_id", how="left")
base = base.join(wearable_agg,          on="global_id", how="left")
base = base.join(consent_agg,           on="global_id", how="left")

base = base.fillna({
    "bank_tx_count": 0,
    "bank_tx_total": 0.0,
    "latest_balance": 0.0,
    "bank_avg_tx_amount": 0.0,
    "card_tx_count": 0,
    "card_total_spend": 0.0,
    "card_category_count": 0,
    "card_avg_spend": 0.0,
    "invest_total": 0.0,
    "invest_product_count": 0,
    "insurance_premium": 0.0,
    "insurance_count": 0,
    "online_insurance_premium": 0.0,
    "online_insurance_count": 0,
    "hospital_visit_count": 0,
    "diet_score": 50.0,
    "sleep_score": 50.0,
    "calories": 0.0,
    "bmi": 22.0,
    "health_score": 50.0,
    "avg_steps": 0.0,
    "avg_heart_rate": 75.0,
    "avg_stress": 40.0,
    "avg_spo2": 98.0,
    "wearable_flag": "N",
    "consent_ratio": 0.0,
})

base = base.withColumn("dt", lit(date_formatted))

output_path = f"s3://{S3_CURATED_BUCKET}/customer_360_profile/dt={date_formatted}/"
print(f"[customer360] Writing output to {output_path}")

base.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[customer360] Job completed successfully. Output: {output_path}")
spark.stop()
