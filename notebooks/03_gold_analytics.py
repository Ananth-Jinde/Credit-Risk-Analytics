# Databricks notebook source

# MAGIC %md
# MAGIC # 03 — Gold Analytics
# MAGIC
# MAGIC **Layer:** Gold (Business-Ready)
# MAGIC **Source:** Silver Delta tables
# MAGIC **Target:** `credit_risk_analytics.gold.credit_risk_scores`
# MAGIC
# MAGIC This notebook joins all 4 Silver tables and calculates a credit risk
# MAGIC score for each borrower using a weighted multi-criteria model.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/credit-risk-analytics")

from src.analytics.credit_risk_scorer import build_credit_risk_scores
from src.config.settings import SilverConfig, GoldConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Silver Tables

# COMMAND ----------

customers_df = spark.table(SilverConfig.DIM_CUSTOMERS)
loans_df = spark.table(SilverConfig.FACT_LOANS)
repayments_df = spark.table(SilverConfig.FACT_LOAN_REPAYMENTS)
defaulters_df = spark.table(SilverConfig.DIM_LOAN_DEFAULTERS)

print(f"Customers: {customers_df.count():,}")
print(f"Loans: {loans_df.count():,}")
print(f"Repayments: {repayments_df.count():,}")
print(f"Defaulters: {defaulters_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Credit Risk Scores
# MAGIC
# MAGIC Joins all 4 Silver tables, scores each borrower across 3 criteria
# MAGIC (payment history 20%, default history 45%, financial health 35%),
# MAGIC and assigns a letter grade (A through F).

# COMMAND ----------

risk_scores_df = build_credit_risk_scores(spark, customers_df, loans_df, repayments_df, defaulters_df)
display(risk_scores_df.limit(20))

# COMMAND ----------

(
    risk_scores_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GoldConfig.CREDIT_RISK_SCORES)
)
print(f"✅ {GoldConfig.CREDIT_RISK_SCORES} written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Results

# COMMAND ----------

# Grade distribution — how many borrowers in each grade?
display(
    risk_scores_df
    .groupBy("credit_risk_grade")
    .count()
    .orderBy("credit_risk_grade")
)

# COMMAND ----------

count = spark.table(GoldConfig.CREDIT_RISK_SCORES).count()
print(f"✅ Gold layer complete — {GoldConfig.CREDIT_RISK_SCORES}: {count:,} records")
