"""
Silver Layer — Loan Defaulter Dimension Transformer.

Extracts delinquency and public record data from Bronze, derives
risk tier classifications, and produces the silver.dim_loan_defaulters
Delta table.

Key transformations:
  - Delinquency aging bucketing
  - Multi-dimensional risk tier assignment
  - Compound risk indicator calculation
  - Null handling with explicit defaults
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.spark_utils import deduplicate_by_key
from src.utils.logger import get_logger

logger = get_logger(__name__)


def extract_defaulter_columns(df: DataFrame) -> DataFrame:
    """Extract default-related columns from Bronze.

    Args:
        df: Bronze layer DataFrame.

    Returns:
        DataFrame with defaulter columns only.
    """
    return df.select(
        F.col("member_id"),
        F.col("delinq_2yrs").cast(IntegerType()).alias("delinq_2yrs"),
        F.col("delinq_amnt").alias("delinq_amount"),
        F.col("mths_since_last_delinq").cast(IntegerType()).alias("months_since_last_delinq"),
        F.col("pub_rec").cast(IntegerType()).alias("public_records"),
        F.col("pub_rec_bankruptcies").cast(IntegerType()).alias("public_bankruptcies"),
        F.col("inq_last_6mths").cast(IntegerType()).alias("inquiries_last_6months"),
        F.col("acc_now_delinq").cast(IntegerType()).alias("accounts_now_delinquent"),
        F.col("collections_12_mths_ex_med").cast(IntegerType()).alias("collections_12months"),
        F.col("chargeoff_within_12_mths").cast(IntegerType()).alias("chargeoffs_12months"),
        F.col("tax_liens").cast(IntegerType()).alias("tax_liens"),
    )


def classify_delinquency_aging(df: DataFrame) -> DataFrame:
    """Bucket months_since_last_delinq into aging categories.

    Categories: NEVER (null = no delinquency), RECENT (<12mo),
    MODERATE (12-36mo), AGED (36-60mo), VERY_OLD (>60mo)

    Args:
        df: Defaulter DataFrame.

    Returns:
        DataFrame with delinquency_aging column.
    """
    months = F.col("months_since_last_delinq")

    return df.withColumn(
        "delinquency_aging",
        F.when(months.isNull(), "NEVER_DELINQUENT")
        .when(months < 12, "RECENT")
        .when((months >= 12) & (months < 36), "MODERATE")
        .when((months >= 36) & (months < 60), "AGED")
        .when(months >= 60, "VERY_OLD")
        .otherwise("UNKNOWN"),
    )


def assign_risk_tier(df: DataFrame) -> DataFrame:
    """Assign a multi-dimensional risk tier based on delinquency signals.

    Risk tiers:
      - VERY_HIGH: Multiple active delinquencies or recent bankruptcies
      - HIGH: Past delinquencies with recent inquiries
      - MODERATE: Minor historical issues
      - LOW: Clean record

    Args:
        df: Defaulter DataFrame.

    Returns:
        DataFrame with risk_tier column.
    """
    return df.withColumn(
        "default_risk_tier",
        F.when(
            (F.coalesce(F.col("delinq_2yrs"), F.lit(0)) > 5)
            | (F.coalesce(F.col("public_bankruptcies"), F.lit(0)) > 2)
            | (F.coalesce(F.col("accounts_now_delinquent"), F.lit(0)) > 0),
            "VERY_HIGH",
        )
        .when(
            (F.coalesce(F.col("delinq_2yrs"), F.lit(0)).between(3, 5))
            | (F.coalesce(F.col("public_records"), F.lit(0)) > 3)
            | (F.coalesce(F.col("chargeoffs_12months"), F.lit(0)) > 0),
            "HIGH",
        )
        .when(
            (F.coalesce(F.col("delinq_2yrs"), F.lit(0)).between(1, 2))
            | (F.coalesce(F.col("public_records"), F.lit(0)).between(1, 3))
            | (F.coalesce(F.col("inquiries_last_6months"), F.lit(0)) > 5),
            "MODERATE",
        )
        .otherwise("LOW"),
    )


def calculate_compound_risk_indicator(df: DataFrame) -> DataFrame:
    """Calculate a compound risk indicator from multiple default signals.

    The compound indicator is a weighted sum of normalized default metrics.
    Higher values = higher risk.

    Args:
        df: Defaulter DataFrame.

    Returns:
        DataFrame with compound_risk_indicator column (0-100 scale).
    """
    return df.withColumn(
        "compound_risk_indicator",
        F.round(
            (
                F.coalesce(F.col("delinq_2yrs"), F.lit(0)) * 15
                + F.coalesce(F.col("public_records"), F.lit(0)) * 20
                + F.coalesce(F.col("public_bankruptcies"), F.lit(0)) * 30
                + F.coalesce(F.col("inquiries_last_6months"), F.lit(0)) * 5
                + F.coalesce(F.col("collections_12months"), F.lit(0)) * 15
                + F.coalesce(F.col("chargeoffs_12months"), F.lit(0)) * 25
                + F.coalesce(F.col("tax_liens"), F.lit(0)) * 20
            ),
            2,
        ),
    )


def transform_defaulters(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """Full Silver defaulter transformation pipeline.

    Args:
        spark: Active SparkSession.
        bronze_df: Bronze layer DataFrame.

    Returns:
        Transformed defaulter DataFrame ready for Delta write.
    """
    logger.info("=" * 60)
    logger.info("SILVER — DIM LOAN DEFAULTERS TRANSFORMATION — START")
    logger.info("=" * 60)

    # Step 1: Extract defaulter columns
    defaulters_df = extract_defaulter_columns(bronze_df)
    logger.info(f"Defaulter columns extracted: {defaulters_df.count():,} records")

    # Step 2: Filter nulls on member_id
    defaulters_df = defaulters_df.filter(F.col("member_id").isNotNull())

    # Step 3: Deduplicate by member_id
    defaulters_df = deduplicate_by_key(
        defaulters_df, ["member_id"], "delinq_2yrs", ascending=False,
    )
    logger.info(f"After deduplication: {defaulters_df.count():,} records")

    # Step 4: Classify delinquency aging
    defaulters_df = classify_delinquency_aging(defaulters_df)

    # Step 5: Assign risk tier
    defaulters_df = assign_risk_tier(defaulters_df)

    # Step 6: Calculate compound risk indicator
    defaulters_df = calculate_compound_risk_indicator(defaulters_df)

    # Step 7: Add transformation timestamp
    defaulters_df = defaulters_df.withColumn("_transformed_at", F.current_timestamp())

    logger.info(f"Final dim_loan_defaulters: {defaulters_df.count():,} records")
    logger.info("SILVER — DIM LOAN DEFAULTERS TRANSFORMATION — COMPLETE")
    return defaulters_df
