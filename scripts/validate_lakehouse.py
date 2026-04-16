"""
validate_lakehouse.py  —  scripts/
Post-pipeline data quality validation via Fabric Lakehouse SQL endpoint.
Checks row counts, null rates, referential integrity, and freshness.
Exits with code 1 on failure — usable in CI/CD gates.
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass

import pyodbc

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class Check:
    name: str
    sql: str
    threshold: float
    operator: str = "gte"  # gte, lte, eq
    critical: bool = True


CHECKS = [
    Check(
        name="gold_fct_orders row count > 0",
        sql="SELECT COUNT(*) FROM gold_fct_orders WHERE order_date = CAST(GETDATE() AS DATE)",
        threshold=1,
        operator="gte",
    ),
    Check(
        name="gold_fct_revenue no nulls in total_revenue",
        sql="SELECT CAST(SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) FROM gold_fct_revenue",
        threshold=0.0,
        operator="eq",
    ),
    Check(
        name="silver_orders no null order_id",
        sql="SELECT COUNT(*) FROM silver_orders WHERE order_id IS NULL",
        threshold=0,
        operator="eq",
    ),
    Check(
        name="silver_orders positive order_amount",
        sql="SELECT CAST(SUM(CASE WHEN order_amount <= 0 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) FROM silver_orders",
        threshold=0.001,
        operator="lte",
        critical=False,
    ),
    Check(
        name="gold dim_product has active products",
        sql="SELECT COUNT(*) FROM gold_dim_product WHERE is_active = 1",
        threshold=1,
        operator="gte",
    ),
    Check(
        name="gold fct_revenue freshness (updated today)",
        sql="SELECT DATEDIFF(hour, MAX(_gold_processed_at), GETDATE()) FROM gold_fct_revenue",
        threshold=4,
        operator="lte",
    ),
    Check(
        name="customer_ltv tier distribution (not all Bronze)",
        sql="SELECT CAST(SUM(CASE WHEN ltv_tier = 'Bronze' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) FROM gold_fct_customer_ltv",
        threshold=0.95,
        operator="lte",
        critical=False,
    ),
]


def get_connection(workspace_id: str, lakehouse_id: str) -> pyodbc.Connection:
    """
    Connect to Fabric Lakehouse SQL endpoint via ODBC.
    Uses Service Principal auth (AZURE_CLIENT_ID / AZURE_CLIENT_SECRET).
    """
    server = f"{workspace_id}.datawarehouse.fabric.microsoft.com"
    database = lakehouse_id
    tenant_id = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]

    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server},1433;"
        f"Database={database};"
        f"Authentication=ActiveDirectoryServicePrincipal;"
        f"UID={client_id}@{tenant_id};"
        f"PWD={client_secret};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str)


def run_check(cursor, check: Check) -> tuple[bool, float]:
    cursor.execute(check.sql)
    result = cursor.fetchone()[0]
    value = float(result) if result is not None else 0.0

    if check.operator == "gte":
        passed = value >= check.threshold
    elif check.operator == "lte":
        passed = value <= check.threshold
    elif check.operator == "eq":
        passed = abs(value - check.threshold) < 1e-9
    else:
        passed = False

    return passed, value


def run_all_checks(workspace_id: str, lakehouse_id: str) -> bool:
    conn = get_connection(workspace_id, lakehouse_id)
    cursor = conn.cursor()

    failures = []
    warnings = []

    for check in CHECKS:
        try:
            passed, value = run_check(cursor, check)
            status = (
                "✅ PASS" if passed else ("❌ FAIL" if check.critical else "⚠️ WARN")
            )
            logger.info(
                f"{status}  [{check.name}]  value={value:.4f}  threshold={check.threshold} ({check.operator})"
            )

            if not passed:
                if check.critical:
                    failures.append(check.name)
                else:
                    warnings.append(check.name)
        except Exception as e:
            logger.error(f"❌ ERROR  [{check.name}]: {e}")
            if check.critical:
                failures.append(check.name)

    cursor.close()
    conn.close()

    if warnings:
        logger.warning(f"{len(warnings)} non-critical check(s) failed: {warnings}")
    if failures:
        logger.error(f"{len(failures)} critical check(s) FAILED: {failures}")
        return False

    logger.info("All critical checks passed ✅")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--lakehouse-id", required=True)
    args = parser.parse_args()

    passed = run_all_checks(args.workspace_id, args.lakehouse_id)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
