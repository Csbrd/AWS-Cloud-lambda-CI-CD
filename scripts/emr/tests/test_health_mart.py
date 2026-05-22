from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, DoubleType, LongType


class TestHealthMart:
    def test_health_grade_boundaries(self, spark):
        """health_score 경계값: <60 RISK, <80 NORMAL, >=80 GOOD"""
        data = [
            (59.9, "RISK"),
            (60.0, "NORMAL"),
            (79.9, "NORMAL"),
            (80.0, "GOOD"),
            (100.0, "GOOD"),
        ]
        df = spark.createDataFrame(data, ["health_score", "expected_grade"])
        df = df.withColumn(
            "health_grade",
            when(col("health_score") < 60, lit("RISK"))
            .when(col("health_score") < 80, lit("NORMAL"))
            .otherwise(lit("GOOD"))
        )
        mismatches = df.filter(col("health_grade") != col("expected_grade")).count()
        assert mismatches == 0

    def test_bmi_category_boundaries(self, spark):
        """BMI 경계값: <18.5 UNDERWEIGHT, <25 NORMAL, <30 OVERWEIGHT, >=30 OBESE"""
        data = [
            (18.4, "UNDERWEIGHT"),
            (18.5, "NORMAL"),
            (24.9, "NORMAL"),
            (25.0, "OVERWEIGHT"),
            (29.9, "OVERWEIGHT"),
            (30.0, "OBESE"),
        ]
        df = spark.createDataFrame(data, ["bmi", "expected_category"])
        df = df.withColumn(
            "bmi_category",
            when(col("bmi") < 18.5, lit("UNDERWEIGHT"))
            .when(col("bmi") < 25.0, lit("NORMAL"))
            .when(col("bmi") < 30.0, lit("OVERWEIGHT"))
            .otherwise(lit("OBESE"))
        )
        mismatches = df.filter(col("bmi_category") != col("expected_category")).count()
        assert mismatches == 0

    def test_fillna_defaults(self, spark):
        """null 필드 → 기본값 채움"""
        schema = StructType([
            StructField("hospital_visit_count", LongType(),   True),
            StructField("hospital_total_cost",  DoubleType(), True),
            StructField("department_count",     LongType(),   True),
            StructField("avg_heart_rate",       DoubleType(), True),
            StructField("avg_steps",            DoubleType(), True),
            StructField("health_score",         DoubleType(), True),
            StructField("bmi",                  DoubleType(), True),
        ])
        df = spark.createDataFrame(
            [(None, None, None, None, None, None, None)],
            schema
        )
        df = df.fillna({
            "hospital_visit_count": 0,
            "hospital_total_cost":  0.0,
            "department_count":     0,
            "avg_heart_rate":       0.0,
            "avg_steps":            0.0,
            "health_score":         50.0,
            "bmi":                  22.0,
        })
        row = df.collect()[0]
        assert row.hospital_visit_count == 0
        assert row.hospital_total_cost == 0.0
        assert row.department_count == 0
        assert row.avg_heart_rate == 0.0
        assert row.avg_steps == 0.0
        assert row.health_score == 50.0
        assert row.bmi == 22.0
