"""
silver_to_gold.py  —  notebooks/
Fabric PySpark Notebook: Silver → Gold transformation.
Builds star schema fact and dimension tables from Silver layer.
Uses MERGE for incremental upserts — safe for daily re-runs.
Optimizes Delta tables with Z-ORDER for query performance.
"""

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ── Configuration ──────────────────────────────────────────────────────────────

SILVER_BASE = "abfss://retail_lakehouse@onelake.dfs.fabric.microsoft.com/Tables"
GOLD_BASE = "abfss://retail_lakehouse@onelake.dfs.fabric.microsoft.com/Tables"
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")

spark = (
    SparkSession.builder.appName("silver-to-gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)


def log(msg: str):
    print(f"[{datetime.utcnow().isoformat()}] {msg}")


def merge_delta(spark, df: DataFrame, target_path: str, merge_key: str):
    """Generic MERGE helper for Gold layer upserts."""
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(df.alias("s"), f"t.{merge_key} = s.{merge_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(target_path)


# ── dim_product ────────────────────────────────────────────────────────────────


def build_dim_product() -> DataFrame:
    log("Building dim_product...")
    df = spark.read.format("delta").load(f"{SILVER_BASE}/silver_products")

    return df.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("sub_category"),
        F.col("brand"),
        F.col("unit_cost"),
        F.col("list_price"),
        F.round(F.col("list_price") - F.col("unit_cost"), 2).alias("gross_margin"),
        F.round(
            (F.col("list_price") - F.col("unit_cost")) / F.col("list_price") * 100, 2
        ).alias("margin_pct"),
        F.col("is_active"),
        F.current_timestamp().alias("_gold_processed_at"),
    ).dropDuplicates(["product_id"])


# ── dim_store ──────────────────────────────────────────────────────────────────


def build_dim_store() -> DataFrame:
    log("Building dim_store...")
    df = spark.read.format("delta").load(f"{SILVER_BASE}/silver_stores")

    return df.select(
        F.col("store_id"),
        F.col("store_name"),
        F.col("region"),
        F.col("state"),
        F.col("city"),
        F.col("store_type"),
        F.col("open_date").cast("date"),
        F.col("is_active"),
        F.current_timestamp().alias("_gold_processed_at"),
    ).dropDuplicates(["store_id"])


# ── fct_orders (grain: one row per order line) ────────────────────────────────


def build_fct_orders(run_date: str) -> DataFrame:
    log("Building fct_orders...")
    orders = (
        spark.read.format("delta")
        .load(f"{SILVER_BASE}/silver_orders")
        .filter(F.col("order_date") == run_date)
    )
    products = spark.read.format("delta").load(f"{GOLD_BASE}/gold_dim_product")
    stores = spark.read.format("delta").load(f"{GOLD_BASE}/gold_dim_store")

    return (
        orders.join(
            products.select("product_id", "category", "margin_pct"),
            on="product_id",
            how="left",
        )
        .join(stores.select("store_id", "region", "state"), on="store_id", how="left")
        .select(
            F.col("order_id"),
            F.col("customer_id_hash"),
            F.col("store_id"),
            F.col("product_id"),
            F.col("region"),
            F.col("state"),
            F.col("category"),
            F.col("quantity"),
            F.col("unit_price"),
            F.col("order_amount"),
            F.round(F.col("order_amount") * F.col("margin_pct") / 100, 2).alias(
                "estimated_margin"
            ),
            F.col("order_date"),
            F.col("order_year"),
            F.col("order_month"),
            F.col("status"),
            F.current_timestamp().alias("_gold_processed_at"),
        )
    )


# ── fct_revenue (grain: region + product_category + day) ─────────────────────


def build_fct_revenue(run_date: str) -> DataFrame:
    log("Building fct_revenue...")
    fct_orders = spark.read.format("delta").load(f"{GOLD_BASE}/gold_fct_orders")

    return (
        fct_orders.filter(F.col("order_date") == run_date)
        .filter(F.col("status") != "CANCELLED")
        .groupBy(
            F.col("order_date"),
            F.col("region"),
            F.col("state"),
            F.col("store_id"),
            F.col("category"),
            F.col("order_year"),
            F.col("order_month"),
        )
        .agg(
            F.sum("order_amount").alias("total_revenue"),
            F.sum("estimated_margin").alias("total_margin"),
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id_hash").alias("unique_customers"),
            F.avg("order_amount").alias("avg_order_value"),
            F.sum("quantity").alias("units_sold"),
        )
        .withColumn(
            "margin_pct",
            F.round(F.col("total_margin") / F.col("total_revenue") * 100, 2),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


# ── fct_customer_ltv (grain: customer) ────────────────────────────────────────


def build_fct_customer_ltv() -> DataFrame:
    log("Building fct_customer_ltv...")
    fct_orders = spark.read.format("delta").load(f"{GOLD_BASE}/gold_fct_orders")

    return (
        fct_orders.filter(F.col("status") != "CANCELLED")
        .groupBy("customer_id_hash")
        .agg(
            F.sum("order_amount").alias("lifetime_value"),
            F.count("order_id").alias("total_orders"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("order_date").alias("active_days"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.datediff(F.max("order_date"), F.min("order_date")).alias(
                "customer_tenure_days"
            ),
        )
        .withColumn(
            "ltv_tier",
            F.when(F.col("lifetime_value") >= 5000, "Platinum")
            .when(F.col("lifetime_value") >= 1000, "Gold")
            .when(F.col("lifetime_value") >= 200, "Silver")
            .otherwise("Bronze"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


# ── Optimize Delta tables ──────────────────────────────────────────────────────


def optimize_gold_tables():
    log("Optimizing Gold Delta tables with Z-ORDER...")
    for table, z_cols in [
        ("gold_fct_orders", "order_date, region"),
        ("gold_fct_revenue", "order_date, region"),
        ("gold_dim_product", "category"),
        ("gold_dim_store", "region"),
    ]:
        spark.sql(f"OPTIMIZE delta.`{GOLD_BASE}/{table}` ZORDER BY ({z_cols})")
        spark.sql(f"VACUUM delta.`{GOLD_BASE}/{table}` RETAIN 168 HOURS")
        log(f"  Optimized {table}")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log(f"=== Silver → Gold | Run date: {RUN_DATE} ===")

    # Dimensions (full refresh — small tables)
    dim_product = build_dim_product()
    merge_delta(spark, dim_product, f"{GOLD_BASE}/gold_dim_product", "product_id")
    log("dim_product done")

    dim_store = build_dim_store()
    merge_delta(spark, dim_store, f"{GOLD_BASE}/gold_dim_store", "store_id")
    log("dim_store done")

    # Facts (incremental by run_date)
    fct_orders = build_fct_orders(RUN_DATE)
    merge_delta(spark, fct_orders, f"{GOLD_BASE}/gold_fct_orders", "order_id")
    log("fct_orders done")

    fct_revenue = build_fct_revenue(RUN_DATE)
    merge_delta(spark, fct_revenue, f"{GOLD_BASE}/gold_fct_revenue", "order_date")
    log("fct_revenue done")

    fct_ltv = build_fct_customer_ltv()
    merge_delta(
        spark, fct_ltv, f"{GOLD_BASE}/gold_fct_customer_ltv", "customer_id_hash"
    )
    log("fct_customer_ltv done")

    optimize_gold_tables()

    log("=== Silver → Gold COMPLETE ===")
