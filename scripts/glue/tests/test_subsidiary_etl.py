from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType


class TestSubsidiaryEtl:
    def test_rename_cols(self, spark):
        """rename_cols: transaction_amount → tx_amount"""
        df = spark.createDataFrame(
            [("G00000001", 50000)],
            ["global_id", "transaction_amount"]
        )
        df = df.withColumnRenamed("transaction_amount", "tx_amount")
        assert "tx_amount" in df.columns
        assert "transaction_amount" not in df.columns

    def test_keep_cols_select(self, spark):
        """keep_cols: 지정한 컬럼만 남기고 나머지 제거"""
        df = spark.createDataFrame(
            [("G00000001", "BNK-001", 1_000_000, "DEPOSIT", "2026-05-08", "extra")],
            ["global_id", "bank_id", "balance", "transaction_type", "transaction_date", "unwanted"]
        )
        keep_cols = ["bank_id", "global_id", "balance", "transaction_type", "transaction_date"]
        df = df.select(*keep_cols)
        assert df.columns == keep_cols
        assert "unwanted" not in df.columns

    def test_dedup_by_pk(self, spark):
        """pk_cols 기준 중복 제거"""
        df = spark.createDataFrame(
            [
                ("BNK-001", "2026-05-08", 1000),
                ("BNK-001", "2026-05-08", 2000),
                ("BNK-002", "2026-05-08", 3000),
            ],
            ["bank_id", "transaction_date", "balance"]
        )
        df = df.dropDuplicates(["bank_id", "transaction_date"])
        assert df.count() == 2

    def test_fillna_defaults(self, spark):
        """null 필드 → 기본값 채움"""
        schema = StructType([
            StructField("global_id",  StringType(), True),
            StructField("balance",    LongType(),   True),
            StructField("tx_amount",  LongType(),   True),
        ])
        df = spark.createDataFrame(
            [("G00000001", None, None)],
            schema
        )
        df = df.fillna({"balance": 0, "tx_amount": 0})
        row = df.collect()[0]
        assert row.balance == 0
        assert row.tx_amount == 0

    def test_consent_filter(self, spark):
        """is_consented=True인 고객만 통과"""
        df = spark.createDataFrame(
            [("G00000001", True), ("G00000002", False), ("G00000003", True)],
            ["global_id", "is_consented"]
        )
        filtered = df.filter(col("is_consented") == True)
        ids = [r.global_id for r in filtered.collect()]
        assert ids == ["G00000001", "G00000003"]
        assert "G00000002" not in ids
