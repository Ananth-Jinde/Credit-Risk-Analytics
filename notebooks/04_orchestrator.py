# Databricks notebook source

# MAGIC %md
# MAGIC # 04 — Pipeline Orchestrator (Development / Testing)
# MAGIC
# MAGIC Runs the full pipeline end-to-end: Bronze → Silver → Gold.
# MAGIC
# MAGIC **This notebook is for development and testing only.**
# MAGIC In production, the pipeline is orchestrated by a **Databricks Lakeflow Job**
# MAGIC defined as-code in `resources/credit_risk_job.yml` and deployed via
# MAGIC **Databricks Asset Bundles** (`databricks bundle deploy`).
# MAGIC The Lakeflow Job runs 3 separate tasks (one per notebook) with
# MAGIC independent retry, monitoring, and a visual DAG.

# COMMAND ----------

import sys
import time
sys.path.append("/Workspace/Repos/credit-risk-analytics")

from src.utils.logger import get_logger
logger = get_logger("orchestrator")

# COMMAND ----------

pipeline_start = time.time()
logger.info("=" * 70)
logger.info("CREDIT RISK ANALYTICS — FULL PIPELINE EXECUTION")
logger.info("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 1: Bronze Ingestion

# COMMAND ----------

stage_start = time.time()

from src.ingestion.bronze_loader import ingest_to_bronze, write_bronze_delta

bronze_df = ingest_to_bronze(spark)
write_bronze_delta(bronze_df, mode="overwrite")

logger.info(f"Stage 1 (Bronze) completed in {time.time() - stage_start:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 2: Silver Transformations

# COMMAND ----------

stage_start = time.time()

from src.transformations.customer_transformer import transform_customers
from src.transformations.loan_transformer import transform_loans
from src.transformations.repayment_transformer import transform_repayments
from src.transformations.defaulter_transformer import transform_defaulters
from src.config.settings import SilverConfig

bronze_table = spark.table("credit_risk_analytics.bronze.raw_loan_data")

# Transform all Silver tables
customers_df = transform_customers(spark, bronze_table)
loans_df = transform_loans(spark, bronze_table)
repayments_df = transform_repayments(spark, bronze_table)
defaulters_df = transform_defaulters(spark, bronze_table)

# Write all Silver tables
for df, table_name in [
    (customers_df, SilverConfig.DIM_CUSTOMERS),
    (loans_df, SilverConfig.FACT_LOANS),
    (repayments_df, SilverConfig.FACT_LOAN_REPAYMENTS),
    (defaulters_df, SilverConfig.DIM_LOAN_DEFAULTERS),
]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    logger.info(f"  Written: {table_name}")

logger.info(f"Stage 2 (Silver) completed in {time.time() - stage_start:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 3: Gold — Credit Risk Scores

# COMMAND ----------

stage_start = time.time()

from src.analytics.credit_risk_scorer import build_credit_risk_scores
from src.config.settings import GoldConfig

# Read Silver tables
customers_df = spark.table(SilverConfig.DIM_CUSTOMERS)
loans_df = spark.table(SilverConfig.FACT_LOANS)
repayments_df = spark.table(SilverConfig.FACT_LOAN_REPAYMENTS)
defaulters_df = spark.table(SilverConfig.DIM_LOAN_DEFAULTERS)

# Build Gold table
risk_scores_df = build_credit_risk_scores(spark, customers_df, loans_df, repayments_df, defaulters_df)
risk_scores_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GoldConfig.CREDIT_RISK_SCORES)
logger.info(f"  Written: {GoldConfig.CREDIT_RISK_SCORES}")

logger.info(f"Stage 3 (Gold) completed in {time.time() - stage_start:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Summary

# COMMAND ----------

total_time = time.time() - pipeline_start

print("=" * 70)
print("PIPELINE EXECUTION COMPLETE")
print("=" * 70)
print(f"Total execution time: {total_time:.1f} seconds")
print()
print("Tables created/refreshed:")
all_tables = [
    "credit_risk_analytics.bronze.raw_loan_data",
    SilverConfig.DIM_CUSTOMERS, SilverConfig.FACT_LOANS,
    SilverConfig.FACT_LOAN_REPAYMENTS, SilverConfig.DIM_LOAN_DEFAULTERS,
    GoldConfig.CREDIT_RISK_SCORES,
]
for table in all_tables:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} records")
print("=" * 70)
