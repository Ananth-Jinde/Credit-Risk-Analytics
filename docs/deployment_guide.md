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

## Step 5: Deploy with Databricks Asset Bundles

DAB deploys your notebook code AND creates the Lakeflow Job — all from the command line.

### 5.1 Install Databricks CLI

```bash
pip install databricks-cli
```

### 5.2 Configure Authentication

```bash
databricks configure --token
# Enter your workspace URL: https://adb-XXXX.azuredatabricks.net
# Enter your Personal Access Token (PAT)
# (Create a PAT in Databricks: User Settings → Developer → Access Tokens → Generate New Token)
```

### 5.3 Update Configuration

1. Open `databricks.yml` in your editor
2. Update the `workspace.host` with your Databricks workspace URL

### 5.4 Validate and Deploy

```bash
# Validate the bundle (checks YAML syntax and references)
databricks bundle validate -t dev

# Deploy to development
databricks bundle deploy -t dev
```

### 5.5 Verify Deployment

1. In Databricks → **"Workflows"** → You should see `[dev] credit_risk_analytics_pipeline` job
2. Click on it → you should see the 3-task DAG: Bronze → Silver → Gold
3. The notebooks should be deployed to your workspace under the bundle path

## Step 6: Run the Pipeline

### Option A — Via the Lakeflow Job (Recommended)

1. In Databricks → **"Workflows"** → Click on `[dev] credit_risk_analytics_pipeline`
2. Click **"Run Now"**
3. Monitor the 3-task DAG — each task shows status independently

### Option B — Run Notebooks Manually

Run the notebooks in order:

| Order | Notebook | What it does | Expected time |
|-------|----------|-------------|---------------|
| 1 | `01_bronze_ingestion.py` | Reads CSV → Bronze Delta table | 3-5 min |
| 2 | `02_silver_transformations.py` | Bronze → 4 Silver tables | 5-8 min |
| 3 | `03_gold_analytics.py` | Silver → Gold credit risk scores | 3-5 min |

Or run `04_orchestrator.py` to execute all 3 stages in one notebook (dev/testing only).

## Step 7: Verify Results

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

## Step 8: Deploy to Production

```bash
# Deploy to production (uses service principal)
databricks bundle deploy -t prod
```

Production deployment uses a service principal (`credit-risk-pipeline-sp`) instead of personal tokens, with proper RBAC permissions.

---

## Troubleshooting

| Issue | Solution |
|-------|---------| 
| `ModuleNotFoundError: No module named 'src'` | Make sure `sys.path.append` in notebooks points to your repo path |
| `CREATE CATALOG` fails on Community Edition | Use `hive_metastore` instead of `credit_risk_analytics` in settings.py |
| CSV file not found | Verify the file path in the Volume matches `bronze_loader.py` |
| Out of memory on cluster | Use a larger cluster or process the CSV in chunks |
| `AnalysisException: Table not found` | Run notebooks in order (01 → 02 → 03) |
| `databricks bundle validate` fails | Check YAML syntax and ensure workspace host is correct |
| DAB deploy fails with auth error | Run `databricks configure --token` and re-enter your PAT |
