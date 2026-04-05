"""
utils.py  —  notebooks/
Shared Spark utilities for Fabric PySpark notebooks.
Helpers for Delta operations, logging, data quality checks,
and Fabric-native notebookutils integration.
"""

import hashlib
import logging
from datetime import datetime
from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def log(msg: str):
    ts = datetime.utcnow().isoformat(timespec="seconds")
    print(f"[{ts}] {msg}")
    logger.info(msg)


def sha256_col(col_name: str):
    """Returns a SHA-256 hashed column expression."""
    return F.udf(lambda v: hashlib.sha256(v.encode()).hexdigest() if v else None)(F.col(col_name))


def count_log(df: DataFrame, label: str) -> DataFrame:
    """Log row count for a DataFrame (triggers an action — use sparingly)."""
    count = df.count()
    log(f"{label}: {count:,} rows")
    return df


def deduplicate(df: DataFrame, partition_cols: list[str], order_col: str = "created_at") -> DataFrame:
    """Keep the latest record per partition key using row_number."""
    from pyspark.sql.window import Window
    window = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def validate_not_null(df: DataFrame, columns: list[str]) -> DataFrame:
    """Drop rows where any of the specified columns is null. Logs dropped count."""
    before = df.count()
    for col in columns:
        df = df.filter(F.col(col).isNotNull())
    after = df.count()
    if before - after > 0:
        log(f"Null validation dropped {before - after:,} rows (checking: {columns})")
    return df


def assert_no_nulls(df: DataFrame, columns: list[str], raise_on_fail: bool = True):
    """Assert no nulls exist in specified columns. Raises ValueError if found."""
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            msg = f"NULL check FAILED: {null_count} nulls in column '{col}'"
            log(f"❌ {msg}")
            if raise_on_fail:
                raise ValueError(msg)
        else:
            log(f"✅ NULL check passed: {col}")


def assert_positive(df: DataFrame, column: str, raise_on_fail: bool = True):
    """Assert all values in column are positive."""
    bad_count = df.filter(F.col(column) <= 0).count()
    if bad_count > 0:
        msg = f"Positive check FAILED: {bad_count} non-positive values in '{column}'"
        log(f"❌ {msg}")
        if raise_on_fail:
            raise ValueError(msg)
    else:
        log(f"✅ Positive check passed: {column}")


def delta_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_key: str,
    partition_cols: list[str] | None = None,
):
    """
    Generic MERGE into a Delta table.
    Creates the table on first run; performs MERGE on subsequent runs.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(source_df.alias("s"), f"t.{merge_key} = s.{merge_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log(f"MERGE complete → {target_path}")
    else:
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)
        log(f"Created Delta table → {target_path}")


def optimize_table(spark: SparkSession, table_path: str, z_order_cols: list[str] | None = None):
    """Run OPTIMIZE + VACUUM on a Delta table."""
    z_order = f"ZORDER BY ({', '.join(z_order_cols)})" if z_order_cols else ""
    spark.sql(f"OPTIMIZE delta.`{table_path}` {z_order}")
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")
    log(f"Optimized {table_path}")


def get_delta_stats(spark: SparkSession, table_path: str) -> dict:
    """Return basic stats for a Delta table."""
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    history = DeltaTable.forPath(spark, table_path).history(1).collect()[0]
    return {
        "num_files": detail["numFiles"],
        "size_bytes": detail["sizeInBytes"],
        "last_modified": str(detail["lastModified"]),
        "last_operation": history["operation"],
        "last_operation_metrics": history["operationMetrics"],
    }


def run_with_retry(fn: Callable, retries: int = 3, delay_seconds: int = 5):
    """Run a function with retry on exception."""
    import time
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            log(f"Attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(delay_seconds * attempt)
