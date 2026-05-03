# Deployment Guide — Credit Risk Analytics

## Prerequisites

- Databricks workspace (Community Edition or paid tier)
- Raw dataset downloaded from [Kaggle](https://www.kaggle.com/datasets/wordsforthewise/lending-club) (`accepted_2007_to_2018q4.csv`)
- Git installed on your local machine

---

## Step 1: Push Code to GitHub

```bash
# Create a new GitHub repo called "credit-risk-analytics"
git init
git add .
git commit -m "Initial commit — Credit Risk Analytics pipeline"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/credit-risk-analytics.git
git push -u origin main
```

## Step 2: Connect GitHub Repo to Databricks

1. Open your Databricks workspace
2. Go to **Workspace → Repos → Add Repo**
3. Paste your GitHub repo URL
4. Click **Create Repo**
5. Your code will be cloned to: `/Workspace/Repos/YOUR_USERNAME/credit-risk-analytics`


## Step 3: Create Unity Catalog Infrastructure

Run the first cell of `notebooks/01_bronze_ingestion.py` — it creates:
- Catalog: `credit_risk_analytics`
- Schemas: `bronze`, `silver`, `gold`
- Volume: `credit_risk_analytics.bronze.landing`

## Step 4: Upload Raw Data

Upload the CSV file to the landing volume:

**Option A — Databricks UI:**
1. Go to **Catalog → credit_risk_analytics → bronze → landing**
2. Click **Upload to this Volume**
3. Upload `accepted_2007_to_2018q4.csv`
4. The file will be at: `/Volumes/credit_risk_analytics/bronze/landing/accepted_2007_to_2018q4.csv`

**Option B — Using the notebook:**
If you have the file accessible via DBFS, you can copy it:
```python
dbutils.fs.cp("dbfs:/FileStore/accepted_2007_to_2018q4.csv",
              "/Volumes/credit_risk_analytics/bronze/landing/accepted_2007_to_2018q4.csv")
```

> **Important**: Make sure the filename in `src/ingestion/bronze_loader.py`
> matches what you uploaded. Update the `SOURCE_FILE` variable if needed.

## Step 5: Run the Pipeline

Run the notebooks in order:

| Order | Notebook | What it does | Expected time |
|-------|----------|-------------|---------------|
| 1 | `01_bronze_ingestion.py` | Reads CSV → Bronze Delta table | 3-5 min |
| 2 | `02_silver_transformations.py` | Bronze → 4 Silver tables | 5-8 min |
| 3 | `03_gold_analytics.py` | Silver → Gold credit risk scores | 3-5 min |

Or run `04_orchestrator.py` to execute all 3 stages in sequence.

## Step 6: Verify Results

After the pipeline completes, verify tables exist:

```sql
-- In a SQL cell or Databricks SQL
SELECT COUNT(*) FROM credit_risk_analytics.bronze.raw_loan_data;
SELECT COUNT(*) FROM credit_risk_analytics.silver.dim_customers;
SELECT COUNT(*) FROM credit_risk_analytics.silver.fact_loans;
SELECT COUNT(*) FROM credit_risk_analytics.silver.fact_loan_repayments;
SELECT COUNT(*) FROM credit_risk_analytics.silver.dim_loan_defaulters;
SELECT COUNT(*) FROM credit_risk_analytics.gold.credit_risk_scores;
```

Expected approximate counts:
- Bronze: ~2.2M records
- Silver tables: ~2.2M each (after deduplication, slightly less for customers)
- Gold: ~2.2M (one score per borrower-loan pair)

## Step 7: Set Up Lakeflow Job (Optional — Production)

For automated daily runs:

1. Go to **Workflows → Create Job**
2. Add **Task 1**: Select `notebooks/01_bronze_ingestion.py`
3. Add **Task 2**: Select `notebooks/02_silver_transformations.py`, set **Depends on: Task 1**
4. Add **Task 3**: Select `notebooks/03_gold_analytics.py`, set **Depends on: Task 2**
5. Configure:
   - **Cluster**: Jobs compute (cheaper than interactive)
   - **Schedule**: e.g., Daily at 2:00 AM IST
   - **Retries**: 2 retries
   - **Alerts**: Add email for failure notifications
6. Click **Create**

---

## Troubleshooting

| Issue | Solution |
|-------|---------|
| `ModuleNotFoundError: No module named 'src'` | Make sure `sys.path.append` in notebooks points to your repo path |
| `CREATE CATALOG` fails on Community Edition | Use `hive_metastore` instead of `credit_risk_analytics` in settings.py |
| CSV file not found | Verify the file path in the Volume matches `bronze_loader.py` |
| Out of memory on cluster | Use a larger cluster or process the CSV in chunks |
| `AnalysisException: Table not found` | Run notebooks in order (01 → 02 → 03) |
