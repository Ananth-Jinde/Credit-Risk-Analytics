# Databricks notebook source

# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC
# MAGIC **Layer:** Bronze (Raw)
# MAGIC **Source:** CSV file from Unity Catalog Volume landing zone
# MAGIC **Target:** `credit_risk_analytics.bronze.raw_loan_data` (Delta)
# MAGIC
# MAGIC This notebook reads the raw accepted_loans.csv, applies explicit schema,
# MAGIC filters garbage rows, generates surrogate member_id, adds audit columns,
# MAGIC and writes to the Bronze Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Catalog, Schemas, and Volume

# COMMAND ----------

# Create the Unity Catalog infrastructure (run once)
spark.sql("CREATE CATALOG IF NOT EXISTS credit_risk_analytics")
spark.sql("CREATE SCHEMA IF NOT EXISTS credit_risk_analytics.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS credit_risk_analytics.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS credit_risk_analytics.gold")

# Create landing volume for raw file uploads
spark.sql("""
    CREATE VOLUME IF NOT EXISTS credit_risk_analytics.bronze.landing
""")

print("✅ Catalog, schemas, and volume created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Upload your CSV
# MAGIC
# MAGIC Upload `accepted_loans.csv` to:
# MAGIC `/Volumes/credit_risk_analytics/bronze/landing/accepted_loans.csv`
# MAGIC
# MAGIC You can do this via the Databricks UI: Catalog → Volumes → Upload

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run Bronze Ingestion Pipeline

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/credit-risk-analytics")

from src.ingestion.bronze_loader import ingest_to_bronze, write_bronze_delta

# Run the full Bronze pipeline
bronze_df = ingest_to_bronze(spark)

# Preview
display(bronze_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Delta Table

# COMMAND ----------

write_bronze_delta(bronze_df, mode="overwrite")
print("✅ Bronze Delta table written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validate Bronze Table

# COMMAND ----------

# Quick validation
bronze_count = spark.table("credit_risk_analytics.bronze.raw_loan_data").count()
print(f"Bronze table row count: {bronze_count:,}")

# Schema check
spark.table("credit_risk_analytics.bronze.raw_loan_data").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Bronze Table

# COMMAND ----------

bronze_table = spark.table("credit_risk_analytics.bronze.raw_loan_data")
row_count = bronze_table.count()

# Check table is not empty
assert row_count > 0, "❌ Bronze table is empty!"

# Check critical columns have no nulls
from pyspark.sql import functions as F
for col_name in ["loan_amnt", "grade", "member_id"]:
    null_count = bronze_table.filter(F.col(col_name).isNull()).count()
    assert null_count == 0, f"❌ Column '{col_name}' has {null_count} nulls!"
    print(f"  ✅ {col_name}: 0 nulls")

print(f"✅ Bronze validation passed — {row_count:,} records, no nulls in critical columns")
