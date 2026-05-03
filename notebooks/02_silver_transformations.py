# Databricks notebook source

# MAGIC %md
# MAGIC # 02 — Silver Transformations
# MAGIC
# MAGIC **Layer:** Silver (Cleaned & Conformed)
# MAGIC **Source:** `credit_risk_analytics.bronze.raw_loan_data` (Delta)
# MAGIC **Targets:**
# MAGIC - `credit_risk_analytics.silver.dim_customers` (SCD Type 2)
# MAGIC - `credit_risk_analytics.silver.fact_loans`
# MAGIC - `credit_risk_analytics.silver.fact_loan_repayments`
# MAGIC - `credit_risk_analytics.silver.dim_loan_defaulters`

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/credit-risk-analytics")

from src.transformations.customer_transformer import transform_customers
from src.transformations.loan_transformer import transform_loans
from src.transformations.repayment_transformer import transform_repayments
from src.transformations.defaulter_transformer import transform_defaulters
from src.config.settings import SilverConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze Table

# COMMAND ----------

bronze_df = spark.table("credit_risk_analytics.bronze.raw_loan_data")
print(f"Bronze records: {bronze_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Transform Customer Dimension (SCD Type 2)

# COMMAND ----------

customers_df = transform_customers(spark, bronze_df)
display(customers_df.limit(20))

# COMMAND ----------

# Write customer dimension
(
    customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SilverConfig.DIM_CUSTOMERS)
)
print(f"✅ {SilverConfig.DIM_CUSTOMERS} written")

# COMMAND ----------

# Validate Silver tables (runs after all transforms are done)
# We'll run the checks after all tables are written — see Step 6 below

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transform Loan Fact Table

# COMMAND ----------

loans_df = transform_loans(spark, bronze_df)
display(loans_df.limit(20))

# COMMAND ----------

(
    loans_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SilverConfig.FACT_LOANS)
)
print(f"✅ {SilverConfig.FACT_LOANS} written")

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Transform Loan Repayments

# COMMAND ----------

repayments_df = transform_repayments(spark, bronze_df)
display(repayments_df.limit(20))

# COMMAND ----------

(
    repayments_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SilverConfig.FACT_LOAN_REPAYMENTS)
)
print(f"✅ {SilverConfig.FACT_LOAN_REPAYMENTS} written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Transform Loan Defaulters Dimension

# COMMAND ----------

defaulters_df = transform_defaulters(spark, bronze_df)
display(defaulters_df.limit(20))

# COMMAND ----------

(
    defaulters_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SilverConfig.DIM_LOAN_DEFAULTERS)
)
print(f"✅ {SilverConfig.DIM_LOAN_DEFAULTERS} written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Validate Silver Tables

# COMMAND ----------

from pyspark.sql import functions as F

# Validate each Silver table
for table_name, pk_col in [
    (SilverConfig.DIM_CUSTOMERS, "member_id"),
    (SilverConfig.FACT_LOANS, "loan_id"),
    (SilverConfig.FACT_LOAN_REPAYMENTS, "loan_id"),
    (SilverConfig.DIM_LOAN_DEFAULTERS, "member_id"),
]:
    df = spark.table(table_name)
    row_count = df.count()
    null_count = df.filter(F.col(pk_col).isNull()).count()
    dup_count = row_count - df.dropDuplicates([pk_col]).count()

    assert row_count > 0, f"\u274c {table_name} is empty!"
    assert null_count == 0, f"\u274c {table_name}.{pk_col} has {null_count} nulls!"
    print(f"  \u2705 {table_name}: {row_count:,} rows, 0 PK nulls, {dup_count} duplicates")

print("\u2705 Silver validation passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("SILVER LAYER — TRANSFORMATION SUMMARY")
print("=" * 60)
for table in [SilverConfig.DIM_CUSTOMERS, SilverConfig.FACT_LOANS,
              SilverConfig.FACT_LOAN_REPAYMENTS, SilverConfig.DIM_LOAN_DEFAULTERS]:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} records")
print("=" * 60)
