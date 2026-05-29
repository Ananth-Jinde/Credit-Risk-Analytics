# Architecture — Credit Risk Analytics

## Pipeline Architecture

![Architecture Diagram](architecture_diagram.png)

## Data Model — Fact & Dimension Tables

![Data Model](data_model_diagram.png)

## What Happens in Each Layer

### Bronze: `raw_loan_data`
- Read CSV with explicit schema (118 columns defined upfront, no `inferSchema`)
- Remove ~33 garbage rows (metadata rows where key columns are NULL)
- Generate `member_id` using SHA-256 hash (raw data has NULL member IDs)
- Add audit columns: `_ingested_at`, `_source_file`, `_batch_id`
- Write to Delta table

### Silver: 4 Domain Tables

**dim_customers** — One row per unique borrower
- Clean employment length: "10+ years" → 10
- Categorize 500K+ job titles into 12 categories (e.g., "Software Engineer" → TECHNOLOGY)
- Validate state codes, add census region (CA → West)
- Cap extreme income outliers above $10M
- Add SCD Type 2 tracking columns

**fact_loans** — One row per loan
- Parse dates: "Dec-2015" → 2015-12-01
- Normalize term: " 36 months" → 36
- Classify interest rates: Low/Medium/High/Very High
- Calculate loan-to-income ratio
- Standardize loan status into categories

**fact_loan_repayments** — One row per loan's repayment record
- Calculate payment completion ratio
- Flag late payments from fee analysis
- Calculate recovery rate for charged-off loans

**dim_loan_defaulters** — One row per borrower's default history
- Assign risk tier: LOW/MODERATE/HIGH/VERY_HIGH
- Bucket delinquency aging
- Calculate compound risk score from multiple signals

### Gold: 1 Analytical Table

**credit_risk_scores** — Per-borrower credit risk grade
- Joins all 4 Silver tables
- Scores across 3 weighted criteria: payment history (20%), default history (45%), financial health (35%)
- Produces a composite score and letter grade (A through F)

## Orchestration & Deployment: DAB + Lakeflow Jobs

The pipeline runs as a **Lakeflow Job** defined as-code via **Databricks Asset Bundles (DAB)**:

```
Task 1: Bronze Ingestion  --->  Task 2: Silver Transforms  --->  Task 3: Gold Analytics
    (01_bronze_ingestion)         (02_silver_transformations)      (03_gold_analytics)
```

- **Job definition**: `resources/credit_risk_job.yml` (tasks, dependencies, schedule, retry, cluster)
- **DAB config**: `databricks.yml` (dev/prod targets, service principal for production)
- **Deploy**: `databricks bundle deploy -t dev` (or `-t prod`)
- **Schedule**: Daily at 2:00 AM IST
- **Retry**: 2 retries on failure with 60s backoff
- **Compute**: Jobs compute cluster (cheaper than interactive)
- **Alerts**: Email notification on failure

DAB handles **full lifecycle management** — both code deployment (notebooks + src/) and orchestration (Lakeflow Job). No manual UI setup needed.
