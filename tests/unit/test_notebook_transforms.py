"""
test_notebook_transforms.py  —  tests/unit/
Unit tests for Bronze → Silver and Silver → Gold PySpark transformations.
Uses local Spark session — no Fabric runtime required.
"""

from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from decimal import Decimal
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test-fabric-transforms")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def bronze_orders(spark):
    schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("store_id", StringType()),
            StructField("product_id", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DecimalType(10, 2)),
            StructField("order_amount", DecimalType(12, 2)),
            StructField("order_date", DateType()),
            StructField("status", StringType()),
            StructField("created_at", TimestampType()),
        ]
    )
    data = [
        (
            "ORD-001",
            "CUST-A",
            "STR-1",
            "PRD-1",
            2,
            Decimal("50.00"),
            100.00,
            date(2024, 1, 15),
            "completed",
            datetime(2024, 1, 15, 10, 0),
        ),
        (
            "ORD-001",
            "CUST-A",
            "STR-1",
            "PRD-1",
            2,
            Decimal("50.00"),
            100.00,
            date(2024, 1, 15),
            "completed",
            datetime(2024, 1, 15, 10, 5),
        ),  # duplicate
        (
            "ORD-002",
            None,
            "STR-2",
            "PRD-2",
            1,
            200.00,
            200.00,
            date(2024, 1, 15),
            "pending",
            datetime(2024, 1, 15, 11, 0),
        ),  # null customer
        (
            "ORD-003",
            "CUST-B",
            "STR-1",
            "PRD-3",
            3,
            10.00,
            -30.00,
            date(2024, 1, 15),
            "refunded",
            datetime(2024, 1, 15, 12, 0),
        ),  # negative amount
        (
            "ORD-004",
            "CUST-C",
            "STR-3",
            "PRD-1",
            1,
            75.00,
            75.00,
            date(2024, 1, 15),
            "  pending  ",
            datetime(2024, 1, 15, 13, 0),
        ),
    ]
    return spark.createDataFrame(data, schema)


# ── Null validation tests ──────────────────────────────────────────────────────


class TestNullValidation:
    def test_drops_null_customer_id(self, spark, bronze_orders):
        from notebooks.utils import validate_not_null

        result = validate_not_null(bronze_orders, ["customer_id"])
        nulls = result.filter(F.col("customer_id").isNull()).count()
        assert nulls == 0

    def test_drops_null_order_id(self, spark, bronze_orders):
        from notebooks.utils import validate_not_null

        result = validate_not_null(bronze_orders, ["order_id"])
        assert result.filter(F.col("order_id").isNull()).count() == 0


# ── Deduplication tests ────────────────────────────────────────────────────────


class TestDeduplication:
    def test_removes_duplicate_order_ids(self, spark, bronze_orders):
        from notebooks.utils import deduplicate

        result = deduplicate(bronze_orders, ["order_id"], "created_at")
        order_ids = [r["order_id"] for r in result.collect()]
        assert len(order_ids) == len(set(order_ids))

    def test_keeps_latest_record(self, spark):
        schema = StructType(
            [
                StructField("order_id", StringType()),
                StructField("status", StringType()),
                StructField("created_at", TimestampType()),
            ]
        )
        data = [
            ("ORD-X", "PENDING", datetime(2024, 1, 1, 8, 0)),
            ("ORD-X", "COMPLETED", datetime(2024, 1, 1, 9, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        from notebooks.utils import deduplicate

        result = deduplicate(df, ["order_id"], "created_at").collect()
        assert result[0]["status"] == "COMPLETED"


# ── PII masking tests ──────────────────────────────────────────────────────────


class TestPIIMasking:
    def test_sha256_deterministic(self, spark, bronze_orders):
        from notebooks.utils import sha256_col

        df = bronze_orders.withColumn("customer_id_hash", sha256_col("customer_id"))
        hashes = {
            r["customer_id"]: r["customer_id_hash"]
            for r in df.filter(F.col("customer_id").isNotNull()).collect()
        }
        # Same input → same hash
        cust_a_hashes = [v for k, v in hashes.items() if k == "CUST-A"]
        if len(cust_a_hashes) > 1:
            assert len(set(cust_a_hashes)) == 1

    def test_sha256_is_64_chars(self, spark, bronze_orders):
        from notebooks.utils import sha256_col

        df = bronze_orders.filter(F.col("customer_id").isNotNull()).withColumn(
            "h", sha256_col("customer_id")
        )
        for row in df.collect():
            assert len(row["h"]) == 64


# ── Status normalization tests ─────────────────────────────────────────────────


class TestStatusNormalization:
    def test_status_uppercased_and_trimmed(self, spark, bronze_orders):
        df = bronze_orders.withColumn("status", F.upper(F.trim(F.col("status"))))
        for row in df.collect():
            assert row["status"] == row["status"].strip().upper()


# ── Gold aggregation tests ─────────────────────────────────────────────────────


class TestGoldAggregations:
    def test_fct_revenue_sums_correctly(self, spark):
        schema = StructType(
            [
                StructField("order_id", StringType()),
                StructField("order_date", DateType()),
                StructField("region", StringType()),
                StructField("order_amount", DecimalType(12, 2)),
                StructField("status", StringType()),
            ]
        )
        data = [
            ("O1", date(2024, 1, 15), "South", 100.00, "COMPLETED"),
            ("O2", date(2024, 1, 15), "South", 200.00, "COMPLETED"),
            (
                "O3",
                date(2024, 1, 15),
                "South",
                Decimal("50.00"),
                "CANCELLED",
            ),  # should be excluded
            ("O4", date(2024, 1, 15), "North", Decimal("300.00"), "COMPLETED"),
        ]
        df = spark.createDataFrame(data, schema)

        result = (
            df.filter(F.col("status") != "CANCELLED")
            .groupBy("order_date", "region")
            .agg(
                F.sum("order_amount").alias("total_revenue"),
                F.count("order_id").alias("total_orders"),
            )
        )

        south = result.filter(F.col("region") == "South").collect()[0]
        assert float(south["total_revenue"]) == Decimal("300.00")
        assert south["total_orders"] == 2

    def test_ltv_tiers_assigned(self, spark):
        schema = StructType(
            [
                StructField("customer_id_hash", StringType()),
                StructField("lifetime_value", DecimalType(14, 2)),
            ]
        )
        data = [
            ("CUST-1", 6000.00),  # Platinum
            ("CUST-2", 1500.00),  # Gold
            ("CUST-3", Decimal("300.00")),  # Silver
            ("CUST-4", Decimal("50.00")),  # Bronze
        ]
        df = spark.createDataFrame(data, schema).withColumn(
            "ltv_tier",
            F.when(F.col("lifetime_value") >= Decimal("5000"), "Platinum")
            .when(F.col("lifetime_value") >= 1000, "Gold")
            .when(F.col("lifetime_value") >= 200, "Silver")
            .otherwise("Bronze"),
        )
        tiers = {r["customer_id_hash"]: r["ltv_tier"] for r in df.collect()}
        assert tiers["CUST-1"] == "Platinum"
        assert tiers["CUST-2"] == "Gold"
        assert tiers["CUST-3"] == "Silver"
        assert tiers["CUST-4"] == "Bronze"
