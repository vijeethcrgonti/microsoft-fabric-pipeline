"""
bronze_to_silver.py  —  notebooks/
Fabric PySpark Notebook: Bronze → Silver transformation.
Reads raw Delta tables from Bronze Lakehouse layer,
applies deduplication, null handling, type casting, PII masking,
and writes to Silver layer with schema enforcement.

Run in Fabric Spark environment — notebookutils available natively.
"""

import hashlib
from datetime import datetime, date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ── Fabric-native: notebookutils available in Fabric Spark runtime ──────────────
# from notebookutils import mssparkutils  # uncomment when running in Fabric

# ── Configuration ──────────────────────────────────────────────────────────────

LAKEHOUSE_NAME = "retail_lakehouse"
BRONZE_BASE = f"abfss://retail_lakehouse@onelake.dfs.fabric.microsoft.com/Tables"
SILVER_BASE = f"abfss://retail_lakehouse@onelake.dfs.fabric.microsoft.com/Tables"

RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")

# ── Spark session (auto-created in Fabric; explicit for local testing) ──────────

spark = SparkSession.builder \
    .appName("bronze-to-silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "16")

# ── Schemas ────────────────────────────────────────────────────────────────────

SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id",         StringType(),       nullable=False),
    StructField("customer_id_hash", StringType(),       nullable=False),
    StructField("store_id",         StringType(),       nullable=True),
    StructField("product_id",       StringType(),       nullable=True),
    StructField("quantity",         IntegerType(),      nullable=True),
    StructField("unit_price",       DecimalType(10, 2), nullable=True),
    StructField("order_amount",     DecimalType(12, 2), nullable=False),
    StructField("order_date",       DateType(),         nullable=False),
    StructField("status",           StringType(),       nullable=True),
    StructField("order_year",       IntegerType(),      nullable=True),
    StructField("order_month",      IntegerType(),      nullable=True),
    StructField("_ingested_at",     TimestampType(),    nullable=True),
    StructField("_processed_at",    TimestampType(),    nullable=True),
])


# ── Helpers ────────────────────────────────────────────────────────────────────

def sha256_udf(col_name: str):
    return F.udf(lambda v: hashlib.sha256(v.encode()).hexdigest() if v else None)(F.col(col_name))


def log(msg: str):
    print(f"[{datetime.utcnow().isoformat()}] {msg}")


# ── Bronze → Silver: Orders ────────────────────────────────────────────────────

def transform_orders(run_date: str) -> DataFrame:
    log("Reading bronze_orders...")
    bronze = spark.read.format("delta").load(f"{BRONZE_BASE}/bronze_orders")

    # Filter to today's partition only (incremental)
    df = bronze.filter(F.col("ingestion_date") == run_date)
    log(f"Bronze rows for {run_date}: {df.count()}")

    # 1. Drop records missing critical fields
    df = (
        df.filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("order_amount").isNotNull())
        .filter(F.col("order_amount") > 0)
        .filter(F.col("order_date").isNotNull())
    )

    # 2. Deduplicate — keep latest by created_at
    window = Window.partitionBy("order_id").orderBy(F.col("created_at").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 3. Type casting + normalization
    df = (
        df.withColumn("order_amount", F.round(F.col("order_amount").cast(DecimalType(12, 2)), 2))
        .withColumn("unit_price", F.round(F.col("unit_price").cast(DecimalType(10, 2)), 2))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("order_date", F.col("order_date").cast(DateType()))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
    )

    # 4. PII masking — SHA-256 customer_id
    df = (
        df.withColumn("customer_id_hash", sha256_udf("customer_id"))
        .drop("customer_id", "email", "phone_number")  # drop raw PII
    )

    # 5. Derived columns
    df = (
        df.withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("_processed_at", F.current_timestamp())
    )

    log(f"Silver orders ready: {df.count()} rows")
    return df.select([f.name for f in SILVER_ORDERS_SCHEMA.fields])


def write_silver_orders(df: DataFrame, run_date: str):
    silver_path = f"{SILVER_BASE}/silver_orders"

    if DeltaTable.isDeltaTable(spark, silver_path):
        log("Merging into existing silver_orders Delta table...")
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("target")
            .merge(df.alias("source"), "target.order_id = source.order_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        log("Creating silver_orders Delta table (first run)...")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("order_year", "order_month")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )

    log("Silver orders write complete")


# ── Bronze → Silver: Products ──────────────────────────────────────────────────

def transform_products() -> DataFrame:
    log("Reading bronze_products...")
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/bronze_products")

    df = (
        df.filter(F.col("product_id").isNotNull())
        .filter(F.col("product_name").isNotNull())
        .dropDuplicates(["product_id"])
        .withColumn("unit_cost", F.round(F.col("unit_cost").cast(DecimalType(10, 2)), 2))
        .withColumn("list_price", F.round(F.col("list_price").cast(DecimalType(10, 2)), 2))
        .withColumn("category", F.upper(F.trim(F.col("category"))))
        .withColumn("is_active", F.col("status") == "ACTIVE")
        .withColumn("_processed_at", F.current_timestamp())
    )

    log(f"Silver products ready: {df.count()} rows")
    return df


def write_silver_products(df: DataFrame):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_BASE}/silver_products")
    )
    log("Silver products write complete")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log(f"=== Bronze → Silver | Run date: {RUN_DATE} ===")

    orders_df = transform_orders(RUN_DATE)
    write_silver_orders(orders_df, RUN_DATE)

    products_df = transform_products()
    write_silver_products(products_df)

    log("=== Bronze → Silver COMPLETE ===")
