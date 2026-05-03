"""
Gold Layer — Credit Risk Scoring Engine.

Implements a configurable, multi-criteria credit risk scoring model
that combines payment history, loan default history, and financial
health into a composite score with a letter grade.

The scoring model is fully configurable via RiskScoringConfig —
weights and thresholds can be adjusted without modifying logic.

Scoring Criteria:
  1. Payment History (20%): Last payment behavior, total payment completion
  2. Default History (45%): Delinquencies, public records, bankruptcies, inquiries
  3. Financial Health (35%): Loan status, home ownership, credit utilization, grade
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.config.settings import RiskScoringConfig as RSC
from src.utils.logger import get_logger

logger = get_logger(__name__)


def score_payment_history(df: DataFrame) -> DataFrame:
    """Score based on payment behavior (Criteria 1 — 20% weight).

    Sub-components:
      - last_payment_pts: Based on last payment vs monthly installment
      - total_payment_pts: Based on total payments vs funded amount

    Args:
        df: Joined DataFrame with loan and repayment data.

    Returns:
        DataFrame with payment history score columns.
    """
    return (
        df
        # Last payment behavior
        .withColumn(
            "last_payment_pts",
            F.when(
                F.col("last_payment_amount") > (F.col("monthly_installment") * 1.5),
                F.lit(RSC.EXCELLENT_PTS),
            )
            .when(
                (F.col("last_payment_amount") > F.col("monthly_installment"))
                & (F.col("last_payment_amount") <= (F.col("monthly_installment") * 1.5)),
                F.lit(RSC.VERY_GOOD_PTS),
            )
            .when(
                F.col("last_payment_amount") == F.col("monthly_installment"),
                F.lit(RSC.GOOD_PTS),
            )
            .when(
                (F.col("last_payment_amount") >= (F.col("monthly_installment") * 0.5))
                & (F.col("last_payment_amount") < F.col("monthly_installment")),
                F.lit(RSC.BAD_PTS),
            )
            .when(
                F.col("last_payment_amount") < (F.col("monthly_installment") * 0.5),
                F.lit(RSC.VERY_BAD_PTS),
            )
            .otherwise(F.lit(RSC.UNACCEPTABLE_PTS)),
        )
        # Total payment completion
        .withColumn(
            "total_payment_pts",
            F.when(
                F.col("payment_completion_ratio") >= 1.0,
                F.lit(RSC.EXCELLENT_PTS),
            )
            .when(
                F.col("payment_completion_ratio") >= 0.5,
                F.lit(RSC.VERY_GOOD_PTS),
            )
            .when(
                (F.col("payment_completion_ratio") > 0)
                & (F.col("payment_completion_ratio") < 0.5),
                F.lit(RSC.GOOD_PTS),
            )
            .when(
                (F.col("payment_completion_ratio") == 0)
                | (F.col("payment_completion_ratio").isNull()),
                F.lit(RSC.UNACCEPTABLE_PTS),
            )
            .otherwise(F.lit(RSC.UNACCEPTABLE_PTS)),
        )
    )


def score_default_history(df: DataFrame) -> DataFrame:
    """Score based on default history (Criteria 2 — 45% weight).

    Sub-components:
      - delinq_pts: Based on delinquencies in past 2 years
      - public_records_pts: Based on public records count
      - bankruptcy_pts: Based on bankruptcy filings
      - inquiry_pts: Based on recent credit inquiries

    Args:
        df: DataFrame with defaulter data.

    Returns:
        DataFrame with default history score columns.
    """
    return (
        df
        .withColumn(
            "delinq_pts",
            F.when(F.coalesce(F.col("delinq_2yrs"), F.lit(0)) == 0, F.lit(RSC.EXCELLENT_PTS))
            .when(F.col("delinq_2yrs").between(1, 2), F.lit(RSC.BAD_PTS))
            .when(F.col("delinq_2yrs").between(3, 5), F.lit(RSC.VERY_BAD_PTS))
            .otherwise(F.lit(RSC.UNACCEPTABLE_PTS)),
        )
        .withColumn(
            "public_records_pts",
            F.when(F.coalesce(F.col("public_records"), F.lit(0)) == 0, F.lit(RSC.EXCELLENT_PTS))
            .when(F.col("public_records").between(1, 2), F.lit(RSC.BAD_PTS))
            .when(F.col("public_records").between(3, 5), F.lit(RSC.VERY_BAD_PTS))
            .otherwise(F.lit(RSC.VERY_BAD_PTS)),
        )
        .withColumn(
            "bankruptcy_pts",
            F.when(F.coalesce(F.col("public_bankruptcies"), F.lit(0)) == 0, F.lit(RSC.EXCELLENT_PTS))
            .when(F.col("public_bankruptcies").between(1, 2), F.lit(RSC.BAD_PTS))
            .when(F.col("public_bankruptcies").between(3, 5), F.lit(RSC.VERY_BAD_PTS))
            .otherwise(F.lit(RSC.UNACCEPTABLE_PTS)),
        )
        .withColumn(
            "inquiry_pts",
            F.when(F.coalesce(F.col("inquiries_last_6months"), F.lit(0)) == 0, F.lit(RSC.EXCELLENT_PTS))
            .when(F.col("inquiries_last_6months").between(1, 2), F.lit(RSC.BAD_PTS))
            .when(F.col("inquiries_last_6months").between(3, 5), F.lit(RSC.VERY_BAD_PTS))
            .otherwise(F.lit(RSC.UNACCEPTABLE_PTS)),
        )
    )


def score_financial_health(df: DataFrame) -> DataFrame:
    """Score based on financial health (Criteria 3 — 35% weight).

    Sub-components:
      - loan_status_pts: Based on current loan status
      - home_ownership_pts: Based on housing stability
      - credit_utilization_pts: Based on funded amount vs credit limit
      - grade_pts: Based on borrower grade (A-G)

    Args:
        df: DataFrame with customer and loan data.

    Returns:
        DataFrame with financial health score columns.
    """
    return (
        df
        # Loan status scoring
        .withColumn(
            "loan_status_pts",
            F.when(F.col("loan_status_category") == "FULLY_PAID", F.lit(RSC.EXCELLENT_PTS))
            .when(F.col("loan_status_category") == "CURRENT", F.lit(RSC.GOOD_PTS))
            .when(F.col("loan_status_category") == "GRACE_PERIOD", F.lit(RSC.BAD_PTS))
            .when(F.col("loan_status_category") == "LATE", F.lit(RSC.VERY_BAD_PTS))
            .when(F.col("loan_status_category") == "CHARGED_OFF", F.lit(RSC.UNACCEPTABLE_PTS))
            .otherwise(F.lit(RSC.UNACCEPTABLE_PTS)),
        )
        # Home ownership stability
        .withColumn(
            "home_ownership_pts",
            F.when(F.upper(F.col("home_ownership")) == "OWN", F.lit(RSC.EXCELLENT_PTS))
            .when(F.upper(F.col("home_ownership")) == "MORTGAGE", F.lit(RSC.GOOD_PTS))
            .when(F.upper(F.col("home_ownership")) == "RENT", F.lit(RSC.BAD_PTS))
            .otherwise(F.lit(RSC.VERY_BAD_PTS)),
        )
        # Credit utilization: funded amount relative to credit limit
        .withColumn(
            "credit_utilization_pts",
            F.when(
                F.col("funded_amount") <= (F.col("total_high_credit_limit") * 0.10),
                F.lit(RSC.EXCELLENT_PTS),
            )
            .when(
                F.col("funded_amount") <= (F.col("total_high_credit_limit") * 0.30),
                F.lit(RSC.VERY_GOOD_PTS),
            )
            .when(
                F.col("funded_amount") <= (F.col("total_high_credit_limit") * 0.50),
                F.lit(RSC.GOOD_PTS),
            )
            .when(
                F.col("funded_amount") <= (F.col("total_high_credit_limit") * 0.70),
                F.lit(RSC.BAD_PTS),
            )
            .otherwise(F.lit(RSC.UNACCEPTABLE_PTS)),
        )
        # Grade-based scoring (A=best, G=worst)
        .withColumn(
            "grade_pts",
            F.when(F.col("grade") == "A", F.lit(RSC.EXCELLENT_PTS))
            .when(F.col("grade") == "B", F.lit(RSC.VERY_GOOD_PTS))
            .when(F.col("grade") == "C", F.lit(RSC.GOOD_PTS))
            .when(F.col("grade") == "D", F.lit(RSC.BAD_PTS))
            .when(F.col("grade") == "E", F.lit(RSC.VERY_BAD_PTS))
            .otherwise(F.lit(RSC.UNACCEPTABLE_PTS)),
        )
    )


def calculate_composite_score(df: DataFrame) -> DataFrame:
    """Calculate the final weighted composite credit risk score.

    Combines the three criteria using configured weights and assigns
    a letter grade based on threshold boundaries.

    Args:
        df: DataFrame with all sub-component scores.

    Returns:
        DataFrame with composite score and final grade.
    """
    return (
        df
        # Weighted sub-scores
        .withColumn(
            "payment_history_score",
            F.round(
                (F.col("last_payment_pts") + F.col("total_payment_pts")) * RSC.PAYMENT_HISTORY_WEIGHT,
                2,
            ),
        )
        .withColumn(
            "default_history_score",
            F.round(
                (F.col("delinq_pts") + F.col("public_records_pts")
                 + F.col("bankruptcy_pts") + F.col("inquiry_pts")) * RSC.DEFAULTER_HISTORY_WEIGHT,
                2,
            ),
        )
        .withColumn(
            "financial_health_score",
            F.round(
                (F.col("loan_status_pts") + F.col("home_ownership_pts")
                 + F.col("credit_utilization_pts") + F.col("grade_pts")) * RSC.FINANCIAL_HEALTH_WEIGHT,
                2,
            ),
        )
        # Composite score
        .withColumn(
            "credit_risk_score",
            F.round(
                F.col("payment_history_score")
                + F.col("default_history_score")
                + F.col("financial_health_score"),
                2,
            ),
        )
        # Final grade assignment
        .withColumn(
            "credit_risk_grade",
            F.when(F.col("credit_risk_score") > RSC.GRADE_A_THRESHOLD, "A")
            .when(F.col("credit_risk_score") > RSC.GRADE_B_THRESHOLD, "B")
            .when(F.col("credit_risk_score") > RSC.GRADE_C_THRESHOLD, "C")
            .when(F.col("credit_risk_score") > RSC.GRADE_D_THRESHOLD, "D")
            .when(F.col("credit_risk_score") > RSC.GRADE_E_THRESHOLD, "E")
            .otherwise("F"),
        )
    )


def build_credit_risk_scores(
    spark: SparkSession,
    customers_df: DataFrame,
    loans_df: DataFrame,
    repayments_df: DataFrame,
    defaulters_df: DataFrame,
) -> DataFrame:
    """Full Gold layer credit risk scoring pipeline.

    Joins all Silver tables, applies scoring across three criteria,
    calculates composite score, and assigns final grade.

    Args:
        spark: Active SparkSession.
        customers_df: Silver dim_customers.
        loans_df: Silver fact_loans.
        repayments_df: Silver fact_loan_repayments.
        defaulters_df: Silver dim_loan_defaulters.

    Returns:
        Gold-layer credit risk scores DataFrame.
    """
    logger.info("=" * 60)
    logger.info("GOLD — CREDIT RISK SCORING — START")
    logger.info("=" * 60)

    # Step 1: Join all Silver tables
    joined_df = (
        loans_df
        .join(customers_df.filter(F.col("is_current") == True), on="member_id", how="inner")
        .join(repayments_df, on=["loan_id", "member_id"], how="inner")
        .join(defaulters_df, on="member_id", how="left")
    )
    logger.info(f"Joined records: {joined_df.count():,}")

    # Step 2: Score payment history
    scored_df = score_payment_history(joined_df)

    # Step 3: Score default history
    scored_df = score_default_history(scored_df)

    # Step 4: Score financial health
    scored_df = score_financial_health(scored_df)

    # Step 5: Calculate composite score and grade
    scored_df = calculate_composite_score(scored_df)

    # Step 6: Select final output columns
    result_df = scored_df.select(
        "member_id",
        "loan_id",
        "grade",
        "annual_income",
        "income_bracket",
        "address_state",
        "address_region",
        "home_ownership",
        "loan_amount",
        "loan_status_category",
        "interest_rate",
        "interest_rate_band",
        "payment_history_score",
        "default_history_score",
        "financial_health_score",
        "credit_risk_score",
        "credit_risk_grade",
        F.current_timestamp().alias("_scored_at"),
    )

    logger.info(f"Final credit risk scores: {result_df.count():,} records")
    logger.info("GOLD — CREDIT RISK SCORING — COMPLETE")
    return result_df
