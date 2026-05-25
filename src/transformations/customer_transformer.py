"""
Silver Layer — Customer Dimension Transformer.

Extracts customer attributes from Bronze, applies advanced cleaning,
derives new features, implements SCD Type 2 logic, and writes to
the silver.dim_customers Delta table.

Key transformations:
  - Employment title standardization (500k+ unique → ~12 categories)
  - Employment length parsing and null imputation (mean fill)
  - Income outlier detection and capping (IQR method)
  - State code validation and region enrichment
  - Derived features: income_bracket, credit_utilization
  - SCD Type 2 tracking for grade, income, and home_ownership changes
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

from src.config.settings import BronzeConfig, SilverConfig, DataQualityConfig
from src.utils.spark_utils import (
    validate_state_udf,
    map_state_to_region_udf,
    deduplicate_by_key,
    classify_income_bracket,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

def extract_customer_columns(df: DataFrame) -> DataFrame:
    """Extract customer-relevant columns from the Bronze dataset.

    Selects only the columns needed for the customer dimension and
    renames them to business-friendly names.

    Args:
        df: Bronze layer DataFrame.

    Returns:
        DataFrame with customer columns only.
    """
    return df.select(
        F.col("member_id"),
        F.col("emp_title"),
        F.col("emp_length"),
        F.col("home_ownership"),
        F.col("annual_inc").alias("annual_income"),
        F.col("addr_state").alias("address_state"),
        F.col("zip_code").alias("address_zipcode"),
        F.lit("USA").alias("address_country"),
        F.col("grade"),
        F.col("sub_grade"),
        F.col("verification_status"),
        F.col("tot_hi_cred_lim").alias("total_high_credit_limit"),
        F.col("application_type"),
        F.col("annual_inc_joint").alias("joint_annual_income"),
        F.col("verification_status_joint"),
    )


def clean_emp_length(df: DataFrame) -> DataFrame:
    """Parse employment length from string to integer and impute nulls.

    Handles: '10+ years' → 10, '< 1 year' → 0, '3 years' → 3, null → mean.

    Args:
        df: Customer DataFrame with string emp_length.

    Returns:
        DataFrame with integer emp_length, nulls replaced by column mean.
    """
    # Extract numeric part
    df = df.withColumn(
        "emp_length",
        F.regexp_extract(F.col("emp_length"), r"(\d+)", 1).cast(IntegerType()),
    )

    # Calculate mean for imputation
    mean_val = df.select(F.mean("emp_length")).collect()[0][0]
    mean_emp_length = int(round(mean_val)) if mean_val else 5

    logger.info(f"Employment length mean for null imputation: {mean_emp_length}")

    # Fill nulls with mean
    df = df.fillna({"emp_length": mean_emp_length})
    return df

# ─────────────────────────────────────────────────────────────────────
# Employment Title Standardization Mapping
# Maps freeform job titles (500k+ unique values) to canonical categories
# ─────────────────────────────────────────────────────────────────────
JOB_CATEGORY_RULES = {
    "TRANSPORTATION": ["driver", "truck", "delivery", "cdl", "logistics", "freight", "pilot"],
    "HEALTHCARE": ["nurse", "doctor", "physician", "medical", "dental", "pharmacy", "therapist", "hospital", "clinical", "health"],
    "EDUCATION": ["teacher", "professor", "instructor", "principal", "school", "education", "tutor", "lecturer"],
    "TECHNOLOGY": ["software", "developer", "engineer", "programmer", "analyst", "data", "tech", "it ", "network", "cyber", "devops"],
    "FINANCE": ["accountant", "finance", "bank", "loan", "credit", "mortgage", "financial", "cpa", "auditor"],
    "MANAGEMENT": ["manager", "director", "supervisor", "lead", "head", "vp ", "president", "chief", "executive"],
    "SALES_MARKETING": ["sales", "marketing", "realtor", "agent", "broker", "retail", "representative"],
    "LEGAL": ["attorney", "lawyer", "paralegal", "legal", "judge"],
    "CONSTRUCTION": ["construction", "electrician", "plumber", "carpenter", "foreman", "mechanic", "welder"],
    "MILITARY_GOV": ["military", "army", "navy", "marine", "police", "officer", "sergeant", "federal", "government", "gs-"],
    "SERVICE": ["server", "bartender", "cook", "chef", "caregiver", "custodian", "housekeeper", "janitor"],
}

def standardize_emp_title(df: DataFrame) -> DataFrame:
    """Standardize freeform employment titles into canonical categories.

    Maps 500k+ unique freeform titles to ~12 high-level categories using
    keyword matching. Titles that don't match any rule are categorized as 'OTHER'.

    Args:
        df: Customer DataFrame with raw emp_title.

    Returns:
        DataFrame with original emp_title and new emp_category column.
    """
    # Build a CASE WHEN expression from the rules
    emp_lower = F.lower(F.trim(F.col("emp_title")))

    # Start with the default
    category_expr = F.lit("OTHER")

    # Apply rules in reverse order so earlier categories have higher priority
    for category, keywords in reversed(list(JOB_CATEGORY_RULES.items())):
        condition = F.lit(False)
        for keyword in keywords:
            condition = condition | emp_lower.contains(keyword)
        category_expr = F.when(condition, F.lit(category)).otherwise(category_expr)

    # Handle null emp_title
    category_expr = F.when(F.col("emp_title").isNull(), F.lit("UNKNOWN")).otherwise(category_expr)

    return df.withColumn("emp_category", category_expr)


def validate_and_enrich_state(df: DataFrame) -> DataFrame:
    """Validate state codes and enrich with region.

    - Invalid state codes → 'NA'
    - Adds `address_region` (Northeast, South, Midwest, West)

    Args:
        df: Customer DataFrame.

    Returns:
        DataFrame with validated state and new region column.
    """
    return (
        df
        .withColumn("address_state", validate_state_udf(F.col("address_state")))
        .withColumn("address_region", map_state_to_region_udf(F.col("address_state")))
    )


def cap_income_outliers(df: DataFrame) -> DataFrame:
    """Cap extreme income outliers using configured thresholds.

    Incomes above the configured maximum are capped (not removed) to
    preserve record count while reducing skew.

    Args:
        df: Customer DataFrame.

    Returns:
        DataFrame with capped annual_income and outlier flag.
    """
    max_income = DataQualityConfig.MAX_ANNUAL_INCOME

    return (
        df
        .withColumn(
            "income_outlier_flag",
            F.when(F.col("annual_income") > max_income, True).otherwise(False),
        )
        .withColumn(
            "annual_income",
            F.when(F.col("annual_income") > max_income, max_income)
            .otherwise(F.col("annual_income")),
        )
    )


def add_derived_features(df: DataFrame) -> DataFrame:
    """Add derived features for downstream analytics.

    New columns:
      - income_bracket: Low / Lower-Middle / Middle / Upper-Middle / High
      - credit_utilization_ratio: Ratio of income to credit limit

    Args:
        df: Customer DataFrame.

    Returns:
        DataFrame with derived feature columns.
    """
    return (
        df
        .withColumn("income_bracket", classify_income_bracket("annual_income"))
        .withColumn(
            "credit_utilization_ratio",
            F.when(
                F.col("total_high_credit_limit") > 0,
                F.round(F.col("annual_income") / F.col("total_high_credit_limit"), 4),
            ).otherwise(F.lit(None)),
        )
    )


def add_scd2_columns(df: DataFrame) -> DataFrame:
    """Add SCD Type 2 tracking columns.

    For the initial load, all records are marked as current.
    Subsequent loads will use MERGE logic to handle historical tracking.

    Args:
        df: Customer DataFrame.

    Returns:
        DataFrame with effective_date, end_date, and is_current columns.
    """
    return (
        df
        .withColumn("effective_date", F.current_date())
        .withColumn("end_date", F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
    )


def transform_customers(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """Full Silver customer transformation pipeline.

    Orchestrates all cleaning, enrichment, and SCD2 steps.

    Args:
        spark: Active SparkSession.
        bronze_df: Bronze layer DataFrame.

    Returns:
        Transformed customer DataFrame ready for Delta write.
    """
    logger.info("=" * 60)
    logger.info("SILVER — CUSTOMER DIMENSION TRANSFORMATION — START")
    logger.info("=" * 60)

    # Step 1: Extract customer columns
    customers_df = extract_customer_columns(bronze_df)
    logger.info(f"Customer columns extracted: {customers_df.count():,} records")

    # Step 2: Remove rows with null income (critical field)
    customers_df = customers_df.filter(F.col("annual_income").isNotNull())
    logger.info(f"After null income filter: {customers_df.count():,} records")

    # Step 3: Deduplicate by member_id (keep first occurrence)
    customers_df = deduplicate_by_key(customers_df, ["member_id"], "annual_income", ascending=False)
    logger.info(f"After deduplication: {customers_df.count():,} records")

    # Step 4: Clean employment length
    customers_df = clean_emp_length(customers_df)

    # Step 5: Standardize employment titles
    customers_df = standardize_emp_title(customers_df)

    # Step 6: Validate state codes and enrich with region
    customers_df = validate_and_enrich_state(customers_df)

    # Step 7: Cap income outliers
    customers_df = cap_income_outliers(customers_df)

    # Step 8: Add derived features
    customers_df = add_derived_features(customers_df)

    # Step 9: Add SCD Type 2 columns
    customers_df = add_scd2_columns(customers_df)

    # Step 10: Add ingestion timestamp
    customers_df = customers_df.withColumn("_transformed_at", F.current_timestamp())

    logger.info(f"Final customer dimension: {customers_df.count():,} records")
    logger.info("SILVER — CUSTOMER DIMENSION TRANSFORMATION — COMPLETE")
    return customers_df
