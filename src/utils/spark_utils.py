"""
Spark utility functions for the Credit Risk Analytics Platform.

Provides reusable helpers for SparkSession creation (local testing vs Databricks),
common DataFrame operations, and UDFs used across transformations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List, Optional

from src.config.settings import STATE_REGION_MAP, VALID_STATE_CODES


def get_spark_session(app_name: str = "CreditRiskAnalytics") -> SparkSession:
    """Get or create a SparkSession.

    On Databricks, the session is pre-configured; this simply returns it.
    For local testing (pytest), this creates a local-mode session with
    Delta Lake support.

    Args:
        app_name: Application name for the Spark UI.

    Returns:
        Active SparkSession instance.
    """
    builder = SparkSession.builder.appName(app_name)

    # Local mode detection: if not on Databricks, configure local Spark
    try:
        # On Databricks, dbutils exists
        from pyspark.dbutils import DBUtils  # noqa: F401
    except (ImportError, ModuleNotFoundError):
        builder = (
            builder
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
        )

    return builder.getOrCreate()


def add_audit_columns(df: DataFrame, source_file: str = "manual") -> DataFrame:
    """Add standard audit columns to a DataFrame.

    Every table in the platform carries these metadata columns for lineage
    tracking and debugging.

    Args:
        df: Input DataFrame.
        source_file: Identifier for the source file or pipeline run.

    Returns:
        DataFrame with _ingested_at, _source_file, and _batch_id columns.
    """
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss")))
    )


def validate_state_code(state: Optional[str]) -> str:
    """Validate and normalize US state codes.

    Returns the state code if valid, otherwise 'NA'.
    """
    if state and state.strip().upper() in VALID_STATE_CODES:
        return state.strip().upper()
    return "NA"


# Register as UDF for use in Spark SQL and DataFrame operations
validate_state_udf = F.udf(validate_state_code, StringType())


def map_state_to_region(state: Optional[str]) -> Optional[str]:
    """Map a US state code to its census region."""
    if state:
        return STATE_REGION_MAP.get(state.strip().upper())
    return None


map_state_to_region_udf = F.udf(map_state_to_region, StringType())


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_column: str,
    ascending: bool = False,
) -> DataFrame:
    """Deduplicate a DataFrame using window functions.

    Keeps the first row per partition (defined by key_columns) when ordered
    by order_column. This is the production-grade approach vs `.distinct()`.

    Args:
        df: Input DataFrame.
        key_columns: Columns that define uniqueness.
        order_column: Column to order by within each partition.
        ascending: Sort order (False = keep latest first).

    Returns:
        Deduplicated DataFrame.
    """
    from pyspark.sql.window import Window

    order_expr = F.col(order_column).asc() if ascending else F.col(order_column).desc()
    window = Window.partitionBy(*key_columns).orderBy(order_expr)

    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def classify_income_bracket(income_col: str = "annual_income") -> F.Column:
    """Classify annual income into business-meaningful brackets.

    Returns a Column expression that can be used in .withColumn().
    """
    return (
        F.when(F.col(income_col) < 30000, "Low")
        .when((F.col(income_col) >= 30000) & (F.col(income_col) < 60000), "Lower-Middle")
        .when((F.col(income_col) >= 60000) & (F.col(income_col) < 100000), "Middle")
        .when((F.col(income_col) >= 100000) & (F.col(income_col) < 200000), "Upper-Middle")
        .when(F.col(income_col) >= 200000, "High")
        .otherwise("Unknown")
    )


def classify_interest_rate_band(rate_col: str = "interest_rate") -> F.Column:
    """Classify interest rate into risk bands."""
    return (
        F.when(F.col(rate_col) < 8.0, "Low")
        .when((F.col(rate_col) >= 8.0) & (F.col(rate_col) < 14.0), "Medium")
        .when((F.col(rate_col) >= 14.0) & (F.col(rate_col) < 20.0), "High")
        .when(F.col(rate_col) >= 20.0, "Very High")
        .otherwise("Unknown")
    )
