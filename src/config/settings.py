"""
Centralized configuration for Credit Risk Analytics.

All catalog names, schema names, volume paths, and table names are defined here.
This ensures zero hardcoded paths across the codebase.
"""


class CatalogConfig:
    """Unity Catalog namespace configuration."""

    CATALOG = "credit_risk_analytics"

    # Schemas (databases) for each Medallion layer
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"

    # Landing zone: Unity Catalog Volume for raw file uploads
    LANDING_VOLUME = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing"


class BronzeConfig:
    """Bronze layer table names and settings."""

    RAW_LOAN_DATA = f"{CatalogConfig.CATALOG}.{CatalogConfig.BRONZE_SCHEMA}.raw_loan_data"

    # Audit columns added during ingestion
    AUDIT_COLUMNS = ["_ingested_at", "_source_file", "_batch_id"]


class SilverConfig:
    """Silver layer table names and settings."""

    _SCHEMA = f"{CatalogConfig.CATALOG}.{CatalogConfig.SILVER_SCHEMA}"

    DIM_CUSTOMERS = f"{_SCHEMA}.dim_customers"
    FACT_LOANS = f"{_SCHEMA}.fact_loans"
    FACT_LOAN_REPAYMENTS = f"{_SCHEMA}.fact_loan_repayments"
    DIM_LOAN_DEFAULTERS = f"{_SCHEMA}.dim_loan_defaulters"

    # SCD Type 2 columns for customer dimension
    SCD2_COLUMNS = ["effective_date", "end_date", "is_current"]


class GoldConfig:
    """Gold layer table names and settings."""

    _SCHEMA = f"{CatalogConfig.CATALOG}.{CatalogConfig.GOLD_SCHEMA}"

    CREDIT_RISK_SCORES = f"{_SCHEMA}.credit_risk_scores"


class RiskScoringConfig:
    """Credit risk scoring model configuration.

    Weights and point thresholds are centralized here so the scoring model
    is fully configurable without touching transformation logic.
    """

    # Component weights (must sum to 1.0)
    PAYMENT_HISTORY_WEIGHT = 0.20
    DEFAULTER_HISTORY_WEIGHT = 0.45
    FINANCIAL_HEALTH_WEIGHT = 0.35

    # Point thresholds for individual sub-scores
    EXCELLENT_PTS = 800
    VERY_GOOD_PTS = 650
    GOOD_PTS = 500
    BAD_PTS = 250
    VERY_BAD_PTS = 100
    UNACCEPTABLE_PTS = 0

    # Grade thresholds for final composite score
    GRADE_A_THRESHOLD = 2500
    GRADE_B_THRESHOLD = 2000
    GRADE_C_THRESHOLD = 1500
    GRADE_D_THRESHOLD = 1000
    GRADE_E_THRESHOLD = 750


class DataQualityConfig:
    """Thresholds for data quality checks."""

    # Maximum allowable null percentage for critical columns
    CRITICAL_NULL_THRESHOLD = 0.0    # member_id, loan_id — zero tolerance
    STANDARD_NULL_THRESHOLD = 0.05   # 5% for most columns
    RELAXED_NULL_THRESHOLD = 0.30    # 30% for optional columns

    # Row count tolerance for layer-to-layer comparison (percentage)
    ROW_COUNT_TOLERANCE = 0.02       # 2% tolerance

    # Numeric range validations
    MIN_ANNUAL_INCOME = 0
    MAX_ANNUAL_INCOME = 10_000_000   # Cap at $10M to flag outliers
    MIN_LOAN_AMOUNT = 0
    MAX_LOAN_AMOUNT = 100_000
    MIN_INTEREST_RATE = 0.0
    MAX_INTEREST_RATE = 35.0


# US State to Region mapping for geographic analytics
STATE_REGION_MAP = {
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast", "NH": "Northeast",
    "RI": "Northeast", "VT": "Northeast", "NJ": "Northeast", "NY": "Northeast",
    "PA": "Northeast",
    "IL": "Midwest", "IN": "Midwest", "MI": "Midwest", "OH": "Midwest",
    "WI": "Midwest", "IA": "Midwest", "KS": "Midwest", "MN": "Midwest",
    "MO": "Midwest", "NE": "Midwest", "ND": "Midwest", "SD": "Midwest",
    "DE": "South", "FL": "South", "GA": "South", "MD": "South",
    "NC": "South", "SC": "South", "VA": "South", "DC": "South",
    "WV": "South", "AL": "South", "KY": "South", "MS": "South",
    "TN": "South", "AR": "South", "LA": "South", "OK": "South", "TX": "South",
    "AZ": "West", "CO": "West", "ID": "West", "MT": "West",
    "NV": "West", "NM": "West", "UT": "West", "WY": "West",
    "AK": "West", "CA": "West", "HI": "West", "OR": "West", "WA": "West",
}

# Valid US state codes for data validation
VALID_STATE_CODES = set(STATE_REGION_MAP.keys())
