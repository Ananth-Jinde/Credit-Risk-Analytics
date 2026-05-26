"""
Silver Layer — Loan Fact Table Transformer.

Extracts loan-specific columns from Bronze, applies date parsing,
term normalization, interest rate banding, derived ratios, and
deduplication to produce the silver.fact_loans Delta table.

Key transformations:
  - Date parsing: 'Dec-2015' string → proper DATE type
  - Term normalization: ' 36 months' → integer 36
  - Interest rate banding into Low/Medium/High/Very High
  - Loan-to-income ratio
  - Funded amount gap (partial funding indicator)
  - Vintage month extraction for cohort analysis
  - Window-function based deduplication on loan_id
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.spark_utils import (
    deduplicate_by_key,
    classify_interest_rate_band,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


def extract_loan_columns(df: DataFrame) -> DataFrame:
    """Extract and rename loan-specific columns from Bronze.

    Args:
        df: Bronze layer DataFrame.

    Returns:
        DataFrame with loan columns only, renamed for clarity.
    """
    return df.select(
        F.col("id").alias("loan_id"),
        F.col("member_id"),
        F.col("loan_amnt").alias("loan_amount"),
        F.col("funded_amnt").alias("funded_amount"),
        F.col("funded_amnt_inv").alias("investor_funded_amount"),
        F.col("term"),
        F.col("int_rate").alias("interest_rate"),
        F.col("installment").alias("monthly_installment"),
        F.col("issue_d").alias("issue_date_raw"),
        F.col("loan_status"),
        F.col("purpose").alias("loan_purpose"),
        F.col("title").alias("loan_title"),
        F.col("annual_inc").alias("annual_income"),
        F.col("grade"),
    )


def parse_loan_dates(df: DataFrame) -> DataFrame:
    """Convert string dates to proper DATE type.

    Handles the 'Mon-YYYY' format (e.g., 'Dec-2015') used in the raw data.

    Args:
        df: Loan DataFrame with string date columns.

    Returns:
        DataFrame with proper DATE columns.
    """
    return (
        df
        .withColumn(
            "issue_date",
            F.to_date(F.col("issue_date_raw"), "MMM-yyyy"),
        )
        .withColumn(
            "issue_year",
            F.year(F.col("issue_date")),
        )
        .withColumn(
            "issue_month",
            F.month(F.col("issue_date")),
        )
        .withColumn(
            "vintage_month",
            F.date_format(F.col("issue_date"), "yyyy-MM"),
        )
        .drop("issue_date_raw")
    )


def normalize_term(df: DataFrame) -> DataFrame:
    """Normalize loan term from string to integer months.

    ' 36 months' → 36, ' 60 months' → 60

    Args:
        df: Loan DataFrame.

    Returns:
        DataFrame with integer loan_term_months column.
    """
    return (
        df
        .withColumn(
            "loan_term_months",
            F.regexp_extract(F.trim(F.col("term")), r"(\d+)", 1).cast(IntegerType()),
        )
        .drop("term")
    )


def standardize_loan_status(df: DataFrame) -> DataFrame:
    """Standardize loan_status values into canonical categories.

    Raw data has variations like 'Fully Paid', 'Current',
    'Late (31-120 days)', 'Charged Off', etc.

    Groups into: FULLY_PAID, CURRENT, LATE, GRACE_PERIOD, DEFAULT, CHARGED_OFF, OTHER

    Args:
        df: Loan DataFrame.

    Returns:
        DataFrame with standardized loan_status_category column.
    """
    status = F.lower(F.trim(F.col("loan_status")))

    return df.withColumn(
        "loan_status_category",
        F.when(status.contains("fully paid"), "FULLY_PAID")
        .when(status.contains("current"), "CURRENT")
        .when(status.contains("grace"), "GRACE_PERIOD")
        .when(status.contains("late"), "LATE")
        .when(status.contains("charged off"), "CHARGED_OFF")
        .when(status.contains("default"), "DEFAULT")
        .otherwise("OTHER"),
    )

def add_loan_derived_features(df: DataFrame) -> DataFrame:
    """Add derived loan features for analytics.

    New columns:
      - interest_rate_band: Low / Medium / High / Very High
      - loan_to_income_ratio: loan_amount / annual_income
      - funded_amount_gap: loan_amount - funded_amount (partial funding)
      - funded_pct: funded_amount / loan_amount (funding completeness)

    Args:
        df: Loan DataFrame.

    Returns:
        DataFrame with derived feature columns.
    """
    return (
        df
        .withColumn("interest_rate_band", classify_interest_rate_band("interest_rate"))
        .withColumn(
            "loan_to_income_ratio",
            F.when(
                (F.col("annual_income").isNotNull()) & (F.col("annual_income") > 0),
                F.round(F.col("loan_amount") / F.col("annual_income"), 4),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "funded_amount_gap",
            F.col("loan_amount") - F.col("funded_amount"),
        )
        .withColumn(
            "funded_pct",
            F.when(
                F.col("loan_amount") > 0,
                F.round(F.col("funded_amount") / F.col("loan_amount"), 4),
            ).otherwise(F.lit(None)),
        )
    )


def transform_loans(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """Full Silver loan transformation pipeline.

    Args:
        spark: Active SparkSession.
        bronze_df: Bronze layer DataFrame.

    Returns:
        Transformed loan DataFrame ready for Delta write.
    """
    logger.info("=" * 60)
    logger.info("SILVER — FACT LOANS TRANSFORMATION — START")
    logger.info("=" * 60)

    # Step 1: Extract loan columns
    loans_df = extract_loan_columns(bronze_df)
    logger.info(f"Loan columns extracted: {loans_df.count():,} records")

    # Step 2: Filter out records with null loan_id
    loans_df = loans_df.filter(F.col("loan_id").isNotNull())

    # Step 3: Deduplicate by loan_id
    loans_df = deduplicate_by_key(loans_df, ["loan_id"], "loan_amount", ascending=False)
    logger.info(f"After deduplication: {loans_df.count():,} records")

    # Step 4: Parse dates
    loans_df = parse_loan_dates(loans_df)

    # Step 5: Normalize term
    loans_df = normalize_term(loans_df)

    # Step 6: Standardize loan status
    loans_df = standardize_loan_status(loans_df)

    # Step 7: Add derived features
    loans_df = add_loan_derived_features(loans_df)

    # Step 8: Drop helper columns used for joining only
    loans_df = loans_df.drop("annual_income", "grade")

    # Step 9: Add transformation timestamp
    loans_df = loans_df.withColumn("_transformed_at", F.current_timestamp())

    logger.info(f"Final fact_loans: {loans_df.count():,} records")
    logger.info("SILVER — FACT LOANS TRANSFORMATION — COMPLETE")
    return loans_df
