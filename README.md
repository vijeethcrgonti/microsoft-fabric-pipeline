# Microsoft Fabric End-to-End Data Pipeline

Production-grade data lakehouse pipeline built entirely on Microsoft Fabric.
Implements the Medallion Architecture (Bronze → Silver → Gold) for retail analytics.
Orchestrated via Fabric Data Factory pipelines, transformed with PySpark Notebooks,
modeled in a Fabric Semantic Model, and visualized in Power BI.
Infrastructure deployed via Bicep (Azure Resource Manager).

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCES                                         │
│  Azure SQL DB (orders)  │  REST API (products)  │  SharePoint (store metadata) │
│  Azure Event Hubs (streaming)  │  ADLS Gen2 (legacy files)                     │
└─────────────┬───────────────────┬──────────────────────┬────────────────────────┘
              │                   │                      │
              ▼                   ▼                      ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                    FABRIC DATA FACTORY (Orchestration)                          │
│   Pipeline: ingest_orders  │  Pipeline: ingest_products  │  Schedule: daily    │
│   Copy Activity → Lakehouse  │  Dataflow Gen2 → Bronze    │  Error alerting    │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│              FABRIC LAKEHOUSE  (OneLake — Delta Lake format)                    │
│                                                                                 │
│  BRONZE  ──►  Raw ingestion, schema-on-read, immutable, full history           │
│               Tables: bronze_orders, bronze_products, bronze_stores             │
│                                                                                 │
│  SILVER  ──►  PySpark Notebooks: cleanse, deduplicate, type-cast, mask PII     │
│               Tables: silver_orders, silver_products, silver_customers          │
│                                                                                 │
│  GOLD    ──►  dbt-style SQL transforms: star schema, aggregates, KPIs          │
│               Tables: fct_revenue, fct_orders, dim_product, dim_store           │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│              FABRIC SEMANTIC MODEL  (DirectLake mode)                           │
│   Relationships  │  DAX measures  │  Row-level security  │  Incremental refresh │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│              POWER BI REPORTS  (Auto-generated via REST API)                    │
│   Revenue by region  │  Product performance  │  Customer LTV  │  Daily KPIs    │
└────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│              MONITORING + CI/CD                                                  │
│   Fabric REST API  │  Azure Monitor  │  GitHub Actions  │  Fabric Git Sync     │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Platform | Microsoft Fabric (F64 SKU) |
| Storage | OneLake (Delta Lake format) |
| Ingestion | Fabric Data Factory + Dataflow Gen2 |
| Compute | Fabric Spark (PySpark Notebooks) |
| SQL Transform | Fabric Warehouse SQL / Lakehouse SQL endpoint |
| Semantic Model | Fabric Semantic Model (DirectLake) |
| Reporting | Power BI (embedded in Fabric) |
| Infrastructure | Azure Bicep |
| CI/CD | GitHub Actions + Fabric REST API |
| Monitoring | Azure Monitor + Fabric Capacity Metrics |

---

## Project Structure

```
microsoft-fabric-pipeline/
├── notebooks/                    # PySpark Notebooks (Bronze → Silver, Silver → Gold)
│   ├── bronze_to_silver.py       # Cleansing, dedup, PII masking
│   ├── silver_to_gold.py         # Aggregations, star schema build
│   └── utils.py                  # Shared Spark helpers
├── dataflows/                    # Dataflow Gen2 definitions (M query / JSON)
│   ├── ingest_orders.json        # Orders from Azure SQL
│   └── ingest_products.json      # Products from REST API
├── pipelines/                    # Fabric Data Factory pipeline definitions (JSON)
│   ├── orchestrate_daily.json    # Master daily pipeline
│   └── ingest_orders.json        # Orders ingestion pipeline
├── lakehouse/
│   ├── bronze/ddl.sql            # Bronze table definitions
│   ├── silver/ddl.sql            # Silver table definitions
│   └── gold/ddl.sql              # Gold star schema DDL + views
├── semantic_model/
│   └── model.bim                 # Semantic model definition (TMSL)
├── reports/                      # Power BI report automation scripts
│   └── create_reports.py         # Fabric REST API report creation
├── infrastructure/bicep/         # Azure Bicep IaC
│   ├── main.bicep
│   └── fabric_capacity.bicep
├── scripts/                      # Admin + deployment scripts
│   ├── deploy_notebooks.py       # Upload notebooks via Fabric REST API
│   ├── trigger_pipeline.py       # Manually trigger pipelines
│   └── validate_lakehouse.py     # Data quality checks via SQL endpoint
└── tests/
    ├── unit/                     # PySpark unit tests
    └── integration/              # End-to-end pipeline validation
```

---

## Setup

### Prerequisites
- Microsoft Fabric workspace (F64 SKU minimum for DirectLake)
- Azure CLI (`az login`)
- Python 3.11+
- Fabric REST API access (Service Principal)

### 1. Configure Environment
```bash
cp .env.example .env
# Fill in: Fabric workspace ID, Lakehouse ID, tenant ID, client ID/secret
```

### 2. Deploy Azure Infrastructure
```bash
cd infrastructure/bicep
az deployment group create \
  --resource-group rg-fabric-pipeline \
  --template-file main.bicep \
  --parameters @parameters.dev.json
```

### 3. Deploy Notebooks to Fabric
```bash
python scripts/deploy_notebooks.py --workspace-id $FABRIC_WORKSPACE_ID
```

### 4. Trigger Full Pipeline
```bash
python scripts/trigger_pipeline.py \
  --pipeline orchestrate_daily \
  --workspace-id $FABRIC_WORKSPACE_ID
```

### 5. Validate Lakehouse Data
```bash
python scripts/validate_lakehouse.py \
  --workspace-id $FABRIC_WORKSPACE_ID \
  --lakehouse-id $FABRIC_LAKEHOUSE_ID
```

---

## Medallion Layer Details

### Bronze
- Raw data landed via Data Factory Copy Activity
- Delta Lake format in OneLake — immutable append-only
- Partitioned by `ingestion_date`
- Schema enforcement OFF (schema-on-read)

### Silver
- PySpark Notebook: deduplication, null handling, type casting
- PII masking: SHA-256 on customer fields
- Schema enforcement ON — rejects non-conforming records
- Partitioned by `year` / `month`

### Gold
- SQL transforms via Lakehouse SQL endpoint
- Star schema: fact tables + dimension tables
- Incremental pattern: `MERGE` on business keys
- Materialized via `CREATE TABLE AS SELECT` + scheduled refresh

---

## Fabric-Specific Features Used

| Feature | Usage |
|---|---|
| OneLake | Single copy of data across all Fabric engines |
| DirectLake | Semantic model reads Delta files directly — no import |
| Dataflow Gen2 | Low-code ingestion with Power Query M |
| Fabric Spark | PySpark notebooks with managed Spark pools |
| Git Integration | Workspace items synced to this GitHub repo |
| Fabric REST API | Programmatic pipeline triggers, notebook deploys |
| Row-Level Security | Semantic model RLS by region/store |
| Incremental Refresh | Power BI report slices refreshed incrementally |

---

## CI/CD Flow (GitHub Actions)

```
Push to main
    │
    ▼
Run unit tests (PySpark local)
    │
    ▼
Validate notebook syntax
    │
    ▼
Deploy notebooks to Fabric (REST API)
    │
    ▼
Trigger orchestrate_daily pipeline
    │
    ▼
Run validate_lakehouse.py (smoke test)
    │
    ▼
Refresh Semantic Model
```

---

## License

MIT
