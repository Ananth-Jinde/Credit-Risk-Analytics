"""
Pytest fixtures for the Credit Risk Analytics Platform test suite.

Provides a shared SparkSession and sample DataFrames that mirror
the schema of each pipeline layer for unit testing.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    BooleanType, DateType, TimestampType,
)
from datetime import date, datetime


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing.

    Uses session scope so one Spark instance is reused across all tests.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("CreditRiskAnalytics_Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_bronze_df(spark):
    """Sample Bronze layer data for testing transformations."""
    data = [
        ("loan_001", "member_001", 10000.0, 10000.0, 10000.0, " 36 months", 12.5,
         350.0, "B", "B2", "Engineer", "5 years", "MORTGAGE", 75000.0,
         "Source Verified", "Dec-2015", "Fully Paid", "n", None, None,
         "debt_consolidation", "Debt Consolidation", "190xx", "PA",
         15.0, 0.0, "Jan-2010", 700.0, 730.0, 1.0, None, None,
         10.0, 0.0, 5000.0, 45.0, 25.0, "f", 0.0, 0.0,
         10000.0, 10000.0, 8000.0, 2000.0, 0.0, 0.0, 0.0,
         "Nov-2018", 350.0, None, "Oct-2018", 720.0, 690.0,
         0.0, None, 1.0, "Individual", None, None, None,
         0.0, 0.0, 20000.0, None, None, None, None, None,
         None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, 0.0,
         0.0, 200000.0, None, None, None, "N", "Cash", "N"),
        ("loan_002", "member_002", 25000.0, 25000.0, 25000.0, " 60 months", 22.5,
         700.0, "E", "E3", "Truck Driver", "10+ years", "RENT", 45000.0,
         "Not Verified", "Mar-2017", "Charged Off", "n", None, None,
         "small_business", "Business", "300xx", "GA",
         25.0, 3.0, "Mar-2005", 650.0, 680.0, 4.0, 24.0, 36.0,
         15.0, 2.0, 12000.0, 65.0, 30.0, "w", 5000.0, 5000.0,
         15000.0, 15000.0, 10000.0, 4500.0, 500.0, 800.0, 200.0,
         "Jan-2019", 200.0, None, "Dec-2018", 640.0, 610.0,
         1.0, 48.0, 1.0, "Individual", None, None, None,
         0.0, 5000.0, 15000.0, None, None, None, None, None,
         None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, 2.0,
         0.0, 100000.0, None, None, None, "N", "Cash", "N"),
    ]

    from src.ingestion.bronze_loader import RAW_LOAN_SCHEMA
    return spark.createDataFrame(data, schema=RAW_LOAN_SCHEMA)


@pytest.fixture
def sample_customer_df(spark):
    """Sample customer DataFrame for testing customer transformations."""
    schema = StructType([
        StructField("member_id", StringType(), True),
        StructField("emp_title", StringType(), True),
        StructField("emp_length", StringType(), True),
        StructField("home_ownership", StringType(), True),
        StructField("annual_income", DoubleType(), True),
        StructField("address_state", StringType(), True),
        StructField("address_zipcode", StringType(), True),
        StructField("address_country", StringType(), True),
        StructField("grade", StringType(), True),
        StructField("sub_grade", StringType(), True),
        StructField("verification_status", StringType(), True),
        StructField("total_high_credit_limit", DoubleType(), True),
        StructField("application_type", StringType(), True),
        StructField("joint_annual_income", DoubleType(), True),
        StructField("verification_status_joint", StringType(), True),
    ])

    data = [
        ("m001", "Software Engineer", "5 years", "MORTGAGE", 120000.0,
         "CA", "900xx", "USA", "A", "A2", "Verified", 300000.0,
         "Individual", None, None),
        ("m002", "TRUCK DRIVER", "10+ years", "RENT", 55000.0,
         "TX", "750xx", "USA", "C", "C3", "Not Verified", 80000.0,
         "Individual", None, None),
        ("m003", "Teacher", "< 1 year", "OWN", 42000.0,
         "NY", "100xx", "USA", "B", "B4", "Source Verified", 150000.0,
         "Individual", None, None),
        ("m004", None, None, "RENT", 85000.0,
         "XY", "000xx", "USA", "D", "D1", "Verified", 200000.0,
         "Individual", None, None),
    ]

    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def sample_loan_df(spark):
    """Sample loan DataFrame for testing loan transformations."""
    schema = StructType([
        StructField("loan_id", StringType(), True),
        StructField("member_id", StringType(), True),
        StructField("loan_amount", DoubleType(), True),
        StructField("funded_amount", DoubleType(), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("monthly_installment", DoubleType(), True),
        StructField("loan_status_category", StringType(), True),
        StructField("loan_purpose", StringType(), True),
        StructField("vintage_month", StringType(), True),
        StructField("interest_rate_band", StringType(), True),
        StructField("loan_to_income_ratio", DoubleType(), True),
        StructField("loan_term_months", IntegerType(), True),
        StructField("issue_date", DateType(), True),
        StructField("funded_amount_gap", DoubleType(), True),
        StructField("funded_pct", DoubleType(), True),
        StructField("_transformed_at", TimestampType(), True),
    ])

    data = [
        ("l001", "m001", 10000.0, 10000.0, 7.5, 350.0, "FULLY_PAID",
         "debt_consolidation", "2015-12", "Low", 0.083, 36,
         date(2015, 12, 1), 0.0, 1.0, datetime.now()),
        ("l002", "m002", 25000.0, 25000.0, 22.5, 700.0, "CHARGED_OFF",
         "small_business", "2017-03", "Very High", 0.556, 60,
         date(2017, 3, 1), 0.0, 1.0, datetime.now()),
    ]

    return spark.createDataFrame(data, schema=schema)
