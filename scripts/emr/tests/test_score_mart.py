from pyspark.sql.functions import col, lit, when, least, greatest


class TestScoreMart:
    def test_grade_boundaries(self, spark):
        """5단계 등급 경계값 검증"""
        data = [
            (90.0, "VIP"),
            (89.9, "GOLD"),
            (80.0, "GOLD"),
            (79.9, "SILVER"),
            (70.0, "SILVER"),
            (69.9, "BASIC"),
            (60.0, "BASIC"),
            (59.9, "CARE"),
            (0.0,  "CARE"),
        ]
        df = spark.createDataFrame(data, ["lifesync_score", "expected_grade"])
        df = df.withColumn(
            "customer_grade",
            when(col("lifesync_score") >= 90, lit("VIP"))
            .when(col("lifesync_score") >= 80, lit("GOLD"))
            .when(col("lifesync_score") >= 70, lit("SILVER"))
            .when(col("lifesync_score") >= 60, lit("BASIC"))
            .otherwise(lit("CARE"))
        )
        mismatches = df.filter(col("customer_grade") != col("expected_grade")).count()
        assert mismatches == 0

    def test_score_capped_at_100(self, spark):
        """raw_score 100 초과 시 lifesync_score는 100으로 제한"""
        df = spark.createDataFrame([(150.0,), (100.0,), (50.0,)], ["raw_score"])
        df = df.withColumn(
            "lifesync_score",
            greatest(lit(0.0), least(lit(100.0), col("raw_score")))
        )
        scores = [r.lifesync_score for r in df.collect()]
        assert scores == [100.0, 100.0, 50.0]

    def test_score_floor_at_0(self, spark):
        """raw_score 음수여도 lifesync_score는 0 이상"""
        df = spark.createDataFrame([(-10.0,), (0.0,)], ["raw_score"])
        df = df.withColumn(
            "lifesync_score",
            greatest(lit(0.0), least(lit(100.0), col("raw_score")))
        )
        scores = [r.lifesync_score for r in df.collect()]
        assert all(s >= 0.0 for s in scores)

    def test_balance_score_component(self, spark):
        """score_balance: 최대 15점, 1M당 1점"""
        df = spark.createDataFrame(
            [(15_000_000,), (1_000_000,), (500_000,)],
            ["latest_balance"]
        )
        df = df.withColumn(
            "score_balance",
            least(lit(15.0), col("latest_balance") / lit(1_000_000.0))
        )
        scores = [r.score_balance for r in df.collect()]
        assert scores[0] == 15.0
        assert scores[1] == 1.0
        assert scores[2] == 0.5

    def test_insurance_score_binary(self, spark):
        """score_insurance: premium > 0이면 5점, 아니면 0점"""
        df = spark.createDataFrame([(100_000,), (0,)], ["insurance_premium"])
        df = df.withColumn(
            "score_insurance",
            when(col("insurance_premium") > 0, lit(5.0)).otherwise(lit(0.0))
        )
        scores = [r.score_insurance for r in df.collect()]
        assert scores == [5.0, 0.0]
