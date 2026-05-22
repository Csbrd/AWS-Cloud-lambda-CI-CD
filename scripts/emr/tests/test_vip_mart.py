from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


class TestVipMart:
    def test_vip_gold_filter(self, spark):
        """VIP/GOLD + wearable_flag=Y 조합만 통과"""
        df = spark.createDataFrame(
            [
                ("G00000001", "VIP",    "Y"),
                ("G00000002", "GOLD",   "Y"),
                ("G00000003", "SILVER", "Y"),
                ("G00000004", "VIP",    "N"),
                ("G00000005", "GOLD",   "N"),
            ],
            ["global_id", "customer_grade", "wearable_flag"]
        )
        filtered = df.filter(
            col("customer_grade").isin("VIP", "GOLD") & (col("wearable_flag") == "Y")
        )
        ids = {r.global_id for r in filtered.collect()}
        assert ids == {"G00000001", "G00000002"}

    def test_total_asset_calc(self, spark):
        """total_asset = latest_balance + invest_total"""
        df = spark.createDataFrame(
            [(5_000_000, 3_000_000)],
            ["latest_balance", "invest_total"]
        )
        df = df.withColumn("total_asset", col("latest_balance") + col("invest_total"))
        assert df.collect()[0].total_asset == 8_000_000

    def test_preferred_subsidiary_securities(self, spark):
        """invest_total이 card, insurance보다 크면 → securities"""
        df = spark.createDataFrame(
            [(10_000_000, 5_000_000, 3_000_000)],
            ["invest_total", "card_total_spend", "insurance_premium"]
        )
        df = df.withColumn(
            "preferred_subsidiary",
            when(
                (col("invest_total") >= col("card_total_spend")) &
                (col("invest_total") >= col("insurance_premium")),
                lit("securities")
            ).when(
                col("card_total_spend") >= col("insurance_premium"),
                lit("card")
            ).otherwise(lit("insurance"))
        )
        assert df.collect()[0].preferred_subsidiary == "securities"

    def test_preferred_subsidiary_card(self, spark):
        """card_total_spend >= insurance_premium이고 invest가 최소이면 → card"""
        df = spark.createDataFrame(
            [(1_000_000, 8_000_000, 3_000_000)],
            ["invest_total", "card_total_spend", "insurance_premium"]
        )
        df = df.withColumn(
            "preferred_subsidiary",
            when(
                (col("invest_total") >= col("card_total_spend")) &
                (col("invest_total") >= col("insurance_premium")),
                lit("securities")
            ).when(
                col("card_total_spend") >= col("insurance_premium"),
                lit("card")
            ).otherwise(lit("insurance"))
        )
        assert df.collect()[0].preferred_subsidiary == "card"

    def test_preferred_subsidiary_insurance(self, spark):
        """insurance_premium이 가장 크면 → insurance"""
        df = spark.createDataFrame(
            [(1_000_000, 2_000_000, 9_000_000)],
            ["invest_total", "card_total_spend", "insurance_premium"]
        )
        df = df.withColumn(
            "preferred_subsidiary",
            when(
                (col("invest_total") >= col("card_total_spend")) &
                (col("invest_total") >= col("insurance_premium")),
                lit("securities")
            ).when(
                col("card_total_spend") >= col("insurance_premium"),
                lit("card")
            ).otherwise(lit("insurance"))
        )
        assert df.collect()[0].preferred_subsidiary == "insurance"

    def test_tx_pattern_boundaries(self, spark):
        """bank_tx_count 경계값: >50 HIGH, >20 MEDIUM, ≤20 LOW"""
        data = [
            (51, "HIGH_FREQUENCY"),
            (50, "MEDIUM_FREQUENCY"),
            (21, "MEDIUM_FREQUENCY"),
            (20, "LOW_FREQUENCY"),
            (0,  "LOW_FREQUENCY"),
        ]
        df = spark.createDataFrame(data, ["bank_tx_count", "expected"])
        df = df.withColumn(
            "tx_pattern",
            when(col("bank_tx_count") > 50, lit("HIGH_FREQUENCY"))
            .when(col("bank_tx_count") > 20, lit("MEDIUM_FREQUENCY"))
            .otherwise(lit("LOW_FREQUENCY"))
        )
        mismatches = df.filter(col("tx_pattern") != col("expected")).count()
        assert mismatches == 0
