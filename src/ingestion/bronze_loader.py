"""
Bronze Layer — Raw Data Ingestion Module.

Reads the raw CSV loan dataset from the Unity Catalog Volume landing zone,
applies an explicit schema (no inferSchema), adds audit metadata columns,
filters out garbage rows, and writes to the Bronze Delta table in append mode.

This module is the single entry point for all raw data into the platform.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

from src.config.settings import CatalogConfig, BronzeConfig
from src.utils.spark_utils import add_audit_columns
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Explicit schema for the raw loan data CSV.
# We define this explicitly instead of using inferSchema for:
#   1. Performance: avoids a full scan to infer types
#   2. Reliability: guarantees consistent types across batches
#   3. Governance: schema-on-read with known expectations
RAW_LOAN_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("loan_amnt", DoubleType(), True),
    StructField("funded_amnt", DoubleType(), True),
    StructField("funded_amnt_inv", DoubleType(), True),
    StructField("term", StringType(), True),
    StructField("int_rate", DoubleType(), True),
    StructField("installment", DoubleType(), True),
    StructField("grade", StringType(), True),
    StructField("sub_grade", StringType(), True),
    StructField("emp_title", StringType(), True),
    StructField("emp_length", StringType(), True),
    StructField("home_ownership", StringType(), True),
    StructField("annual_inc", DoubleType(), True),
    StructField("verification_status", StringType(), True),
    StructField("issue_d", StringType(), True),
    StructField("loan_status", StringType(), True),
    StructField("pymnt_plan", StringType(), True),
    StructField("url", StringType(), True),
    StructField("desc", StringType(), True),
    StructField("purpose", StringType(), True),
    StructField("title", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("addr_state", StringType(), True),
    StructField("dti", DoubleType(), True),
    StructField("delinq_2yrs", DoubleType(), True),
    StructField("earliest_cr_line", StringType(), True),
    StructField("fico_range_low", DoubleType(), True),
    StructField("fico_range_high", DoubleType(), True),
    StructField("inq_last_6mths", DoubleType(), True),
    StructField("mths_since_last_delinq", DoubleType(), True),
    StructField("mths_since_last_record", DoubleType(), True),
    StructField("open_acc", DoubleType(), True),
    StructField("pub_rec", DoubleType(), True),
    StructField("revol_bal", DoubleType(), True),
    StructField("revol_util", DoubleType(), True),
    StructField("total_acc", DoubleType(), True),
    StructField("initial_list_status", StringType(), True),
    StructField("out_prncp", DoubleType(), True),
    StructField("out_prncp_inv", DoubleType(), True),
    StructField("total_pymnt", DoubleType(), True),
    StructField("total_pymnt_inv", DoubleType(), True),
    StructField("total_rec_prncp", DoubleType(), True),
    StructField("total_rec_int", DoubleType(), True),
    StructField("total_rec_late_fee", DoubleType(), True),
    StructField("recoveries", DoubleType(), True),
    StructField("collection_recovery_fee", DoubleType(), True),
    StructField("last_pymnt_d", StringType(), True),
    StructField("last_pymnt_amnt", DoubleType(), True),
    StructField("next_pymnt_d", StringType(), True),
    StructField("last_credit_pull_d", StringType(), True),
    StructField("last_fico_range_high", DoubleType(), True),
    StructField("last_fico_range_low", DoubleType(), True),
    StructField("collections_12_mths_ex_med", DoubleType(), True),
    StructField("mths_since_last_major_derog", DoubleType(), True),
    StructField("policy_code", DoubleType(), True),
    StructField("application_type", StringType(), True),
    StructField("annual_inc_joint", DoubleType(), True),
    StructField("dti_joint", DoubleType(), True),
    StructField("verification_status_joint", StringType(), True),
    StructField("acc_now_delinq", DoubleType(), True),
    StructField("tot_coll_amt", DoubleType(), True),
    StructField("tot_cur_bal", DoubleType(), True),
    StructField("open_acc_6m", DoubleType(), True),
    StructField("open_act_il", DoubleType(), True),
    StructField("open_il_12m", DoubleType(), True),
    StructField("open_il_24m", DoubleType(), True),
    StructField("mths_since_rcnt_il", DoubleType(), True),
    StructField("total_bal_il", DoubleType(), True),
    StructField("il_util", DoubleType(), True),
    StructField("open_rv_12m", DoubleType(), True),
    StructField("open_rv_24m", DoubleType(), True),
    StructField("max_bal_bc", DoubleType(), True),
    StructField("all_util", DoubleType(), True),
    StructField("total_rev_hi_lim", DoubleType(), True),
    StructField("inq_fi", DoubleType(), True),
    StructField("total_cu_tl", DoubleType(), True),
    StructField("inq_last_12m", DoubleType(), True),
    StructField("acc_open_past_24mths", DoubleType(), True),
    StructField("avg_cur_bal", DoubleType(), True),
    StructField("bc_open_to_buy", DoubleType(), True),
    StructField("bc_util", DoubleType(), True),
    StructField("chargeoff_within_12_mths", DoubleType(), True),
    StructField("delinq_amnt", DoubleType(), True),
    StructField("mo_sin_old_il_acct", DoubleType(), True),
    StructField("mo_sin_old_rev_tl_op", DoubleType(), True),
    StructField("mo_sin_rcnt_rev_tl_op", DoubleType(), True),
    StructField("mo_sin_rcnt_tl", DoubleType(), True),
    StructField("mort_acc", DoubleType(), True),
    StructField("mths_since_recent_bc", DoubleType(), True),
    StructField("mths_since_recent_bc_dlq", DoubleType(), True),
    StructField("mths_since_recent_inq", DoubleType(), True),
    StructField("mths_since_recent_revol_delinq", DoubleType(), True),
    StructField("num_accts_ever_120_pd", DoubleType(), True),
    StructField("num_actv_bc_tl", DoubleType(), True),
    StructField("num_actv_rev_tl", DoubleType(), True),
    StructField("num_bc_sats", DoubleType(), True),
    StructField("num_bc_tl", DoubleType(), True),
    StructField("num_il_tl", DoubleType(), True),
    StructField("num_op_rev_tl", DoubleType(), True),
    StructField("num_rev_accts", DoubleType(), True),
    StructField("num_rev_tl_bal_gt_0", DoubleType(), True),
    StructField("num_sats", DoubleType(), True),
    StructField("num_tl_120dpd_2m", DoubleType(), True),
    StructField("num_tl_30dpd", DoubleType(), True),
    StructField("num_tl_90g_dpd_24m", DoubleType(), True),
    StructField("num_tl_op_past_12m", DoubleType(), True),
    StructField("pct_tl_nvr_dlq", DoubleType(), True),
    StructField("percent_bc_gt_75", DoubleType(), True),
    StructField("pub_rec_bankruptcies", DoubleType(), True),
    StructField("tax_liens", DoubleType(), True),
    StructField("tot_hi_cred_lim", DoubleType(), True),
    StructField("total_bal_ex_mort", DoubleType(), True),
    StructField("total_bc_limit", DoubleType(), True),
    StructField("total_il_high_credit_limit", DoubleType(), True),
    StructField("hardship_flag", StringType(), True),
    StructField("disbursement_method", StringType(), True),
    StructField("debt_settlement_flag", StringType(), True),
])


def read_raw_csv(spark: SparkSession, file_path: str = None) -> DataFrame:
    """Read the raw loan CSV file with explicit schema.

    Args:
        spark: Active SparkSession.
        file_path: Path to the CSV file. Defaults to the landing volume.

    Returns:
        Raw DataFrame with explicit schema applied.
    """
    if file_path is None:
        file_path = f"{CatalogConfig.LANDING_VOLUME}/accepted_loans.csv"

    logger.info(f"Reading raw CSV from: {file_path}")

    df = (
        spark.read
        .format("csv")
        .option("header", True)
        .schema(RAW_LOAN_SCHEMA)
        .load(file_path)
    )

    initial_count = df.count()
    logger.info(f"Raw records read: {initial_count:,}")
    return df


def filter_garbage_rows(df: DataFrame) -> DataFrame:
    """Remove rows where all key business columns are null.

    The raw dataset contains ~33 rows that are metadata/description rows
    (e.g., 'Total amount funded...') rather than actual loan records.
    These have null values across all numeric columns.

    Args:
        df: Raw DataFrame.

    Returns:
        Filtered DataFrame with garbage rows removed.
    """
    # A legitimate loan record must have at least loan_amnt AND grade
    critical_columns = ["loan_amnt", "grade", "annual_inc"]

    filtered = df.filter(
        F.col("loan_amnt").isNotNull()
        & F.col("grade").isNotNull()
        & F.col("annual_inc").isNotNull()
    )

    removed = df.count() - filtered.count()
    logger.info(f"Garbage rows removed: {removed:,}")
    return filtered


def generate_member_id(df: DataFrame) -> DataFrame:
    """Generate a deterministic surrogate member_id using SHA-256.

    The original dataset has NULL member_id for all records. We create a
    deterministic ID by hashing a combination of borrower attributes.
    This is a surrogate key — not a natural key.

    Args:
        df: DataFrame with raw columns.

    Returns:
        DataFrame with a generated `member_id` column.
    """
    id_columns = [
        "emp_title", "emp_length", "home_ownership", "annual_inc",
        "zip_code", "addr_state", "grade", "sub_grade", "verification_status",
    ]

    df = df.withColumn(
        "member_id",
        F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("__NULL__")) for c in id_columns]),
            256,
        ),
    )

    return df


def ingest_to_bronze(spark: SparkSession, file_path: str = None) -> DataFrame:
    """Full Bronze ingestion pipeline.

    Orchestrates: read → filter garbage → generate member_id → add audit cols → return.

    Args:
        spark: Active SparkSession.
        file_path: Optional path override for the raw CSV.

    Returns:
        Cleaned and enriched Bronze DataFrame ready for Delta write.
    """
    logger.info("=" * 60)
    logger.info("BRONZE INGESTION PIPELINE — START")
    logger.info("=" * 60)

    # Step 1: Read raw CSV
    raw_df = read_raw_csv(spark, file_path)

    # Step 2: Remove garbage/metadata rows
    clean_df = filter_garbage_rows(raw_df)

    # Step 3: Generate surrogate member_id
    member_df = generate_member_id(clean_df)

    # Step 4: Add audit metadata
    source = file_path or "landing_volume"
    bronze_df = add_audit_columns(member_df, source_file=source)

    logger.info(f"Bronze records ready for write: {bronze_df.count():,}")
    logger.info("BRONZE INGESTION PIPELINE — COMPLETE")
    return bronze_df


def write_bronze_delta(df: DataFrame, table_name: str = None, mode: str = "overwrite"):
    """Write the Bronze DataFrame to a Delta table.

    Args:
        df: Bronze DataFrame.
        table_name: Fully qualified Delta table name.
        mode: Write mode ('overwrite' for initial load, 'append' for incremental).
    """
    if table_name is None:
        table_name = BronzeConfig.RAW_LOAN_DATA

    logger.info(f"Writing to Delta table: {table_name} (mode={mode})")

    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    logger.info(f"Delta write complete: {table_name}")
