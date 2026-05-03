"""
Silver Layer — Loan Repayment Fact Table Transformer.

Extracts repayment information from Bronze, derives payment behavior
metrics, and produces the silver.fact_loan_repayments Delta table.

Key transformations:
  - Date parsing for payment dates
  - Payment completion ratio calculation
  - Late payment flag derivation
  - Recovery rate for charged-off loans
  - Payment velocity metrics
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils.spark_utils import deduplicate_by_key
from src.utils.logger import get_logger

logger = get_logger(__name__)


def extract_repayment_columns(df: DataFrame) -> DataFrame:
    """Extract and rename repayment columns from Bronze.

    Args:
        df: Bronze layer DataFrame.

    Returns:
        DataFrame with repayment columns only.
    """
    return df.select(
        F.col("id").alias("loan_id"),
        F.col("member_id"),
        F.col("loan_amnt").alias("loan_amount"),
        F.col("funded_amnt").alias("funded_amount"),
        F.col("installment").alias("monthly_installment"),
        F.col("total_rec_prncp").alias("total_principal_received"),
        F.col("total_rec_int").alias("total_interest_received"),
        F.col("total_rec_late_fee").alias("total_late_fee_received"),
        F.col("total_pymnt").alias("total_payment_received"),
        F.col("last_pymnt_amnt").alias("last_payment_amount"),
        F.col("last_pymnt_d").alias("last_payment_date_raw"),
        F.col("next_pymnt_d").alias("next_payment_date_raw"),
        F.col("recoveries"),
        F.col("collection_recovery_fee"),
        F.col("issue_d").alias("issue_date_raw"),
        F.col("out_prncp").alias("outstanding_principal"),
    )


def parse_payment_dates(df: DataFrame) -> DataFrame:
    """Parse payment date strings to proper DATE type.

    Args:
        df: Repayment DataFrame with raw date strings.

    Returns:
        DataFrame with parsed date columns.
    """
    return (
        df
        .withColumn("last_payment_date", F.to_date(F.col("last_payment_date_raw"), "MMM-yyyy"))
        .withColumn("next_payment_date", F.to_date(F.col("next_payment_date_raw"), "MMM-yyyy"))
        .withColumn("issue_date", F.to_date(F.col("issue_date_raw"), "MMM-yyyy"))
        .drop("last_payment_date_raw", "next_payment_date_raw", "issue_date_raw")
    )


def add_repayment_derived_features(df: DataFrame) -> DataFrame:
    """Add derived repayment behavior features.

    New columns:
      - payment_completion_ratio: total_payment / (loan_amount + total_interest)
      - late_payment_flag: True if any late fees were charged
      - recovery_rate: recoveries / remaining balance (for charged-off loans)
      - months_since_issue: months between issue_date and last_payment_date
      - payment_to_installment_ratio: last_payment / monthly_installment

    Args:
        df: Repayment DataFrame.

    Returns:
        DataFrame with derived feature columns.
    """
    return (
        df
        # Payment completion ratio
        .withColumn(
            "payment_completion_ratio",
            F.when(
                F.col("loan_amount") > 0,
                F.round(F.col("total_payment_received") / F.col("loan_amount"), 4),
            ).otherwise(F.lit(None)),
        )
        # Late payment flag
        .withColumn(
            "late_payment_flag",
            F.when(
                (F.col("total_late_fee_received").isNotNull())
                & (F.col("total_late_fee_received") > 0),
                True,
            ).otherwise(False),
        )
        # Recovery rate (for loans with outstanding principal)
        .withColumn(
            "recovery_rate",
            F.when(
                (F.col("outstanding_principal") > 0) & (F.col("recoveries").isNotNull()),
                F.round(F.col("recoveries") / F.col("outstanding_principal"), 4),
            ).otherwise(F.lit(0.0)),
        )
        # Months since issue to last payment
        .withColumn(
            "months_on_books",
            F.months_between(F.col("last_payment_date"), F.col("issue_date")).cast("int"),
        )
        # Payment-to-installment ratio (indicates over/under-payment)
        .withColumn(
            "payment_to_installment_ratio",
            F.when(
                (F.col("monthly_installment").isNotNull()) & (F.col("monthly_installment") > 0),
                F.round(F.col("last_payment_amount") / F.col("monthly_installment"), 4),
            ).otherwise(F.lit(None)),
        )
    )


def transform_repayments(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """Full Silver repayment transformation pipeline.

    Args:
        spark: Active SparkSession.
        bronze_df: Bronze layer DataFrame.

    Returns:
        Transformed repayment DataFrame ready for Delta write.
    """
    logger.info("=" * 60)
    logger.info("SILVER — FACT LOAN REPAYMENTS TRANSFORMATION — START")
    logger.info("=" * 60)

    # Step 1: Extract repayment columns
    repayments_df = extract_repayment_columns(bronze_df)
    logger.info(f"Repayment columns extracted: {repayments_df.count():,} records")

    # Step 2: Filter nulls on loan_id
    repayments_df = repayments_df.filter(F.col("loan_id").isNotNull())

    # Step 3: Deduplicate by loan_id
    repayments_df = deduplicate_by_key(
        repayments_df, ["loan_id"], "total_payment_received", ascending=False,
    )
    logger.info(f"After deduplication: {repayments_df.count():,} records")

    # Step 4: Parse dates
    repayments_df = parse_payment_dates(repayments_df)

    # Step 5: Add derived features
    repayments_df = add_repayment_derived_features(repayments_df)

    # Step 6: Drop intermediate columns used for calculation only
    repayments_df = repayments_df.drop("loan_amount", "funded_amount", "issue_date")

    # Step 7: Add transformation timestamp
    repayments_df = repayments_df.withColumn("_transformed_at", F.current_timestamp())

    logger.info(f"Final fact_loan_repayments: {repayments_df.count():,} records")
    logger.info("SILVER — FACT LOAN REPAYMENTS TRANSFORMATION — COMPLETE")
    return repayments_df
