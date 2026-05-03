"""
Unit tests for the Customer Transformer module.

Tests cover:
  - Employment length parsing (string → int)
  - Employment title categorization
  - State code validation
  - Income outlier capping
  - Derived feature generation
"""

import pytest
from pyspark.sql import functions as F
from src.transformations.customer_transformer import (
    clean_emp_length,
    standardize_emp_title,
    validate_and_enrich_state,
    cap_income_outliers,
    add_derived_features,
)


class TestCleanEmpLength:
    """Tests for employment length parsing and imputation."""

    def test_standard_years(self, spark, sample_customer_df):
        """'5 years' should parse to integer 5."""
        result = clean_emp_length(sample_customer_df)
        m001_val = result.filter(F.col("member_id") == "m001").select("emp_length").collect()[0][0]
        assert m001_val == 5

    def test_ten_plus_years(self, spark, sample_customer_df):
        """'10+ years' should parse to integer 10."""
        result = clean_emp_length(sample_customer_df)
        m002_val = result.filter(F.col("member_id") == "m002").select("emp_length").collect()[0][0]
        assert m002_val == 10

    def test_less_than_one_year(self, spark, sample_customer_df):
        """'< 1 year' should parse to integer 1."""
        result = clean_emp_length(sample_customer_df)
        m003_val = result.filter(F.col("member_id") == "m003").select("emp_length").collect()[0][0]
        assert m003_val == 1

    def test_null_imputation(self, spark, sample_customer_df):
        """Null emp_length should be replaced with the column mean."""
        result = clean_emp_length(sample_customer_df)
        m004_val = result.filter(F.col("member_id") == "m004").select("emp_length").collect()[0][0]
        assert m004_val is not None
        assert isinstance(m004_val, int)


class TestStandardizeEmpTitle:
    """Tests for employment title categorization."""

    def test_technology_category(self, spark, sample_customer_df):
        """'Software Engineer' should map to TECHNOLOGY."""
        result = standardize_emp_title(sample_customer_df)
        cat = result.filter(F.col("member_id") == "m001").select("emp_category").collect()[0][0]
        assert cat == "TECHNOLOGY"

    def test_transportation_category(self, spark, sample_customer_df):
        """'TRUCK DRIVER' should map to TRANSPORTATION."""
        result = standardize_emp_title(sample_customer_df)
        cat = result.filter(F.col("member_id") == "m002").select("emp_category").collect()[0][0]
        assert cat == "TRANSPORTATION"

    def test_education_category(self, spark, sample_customer_df):
        """'Teacher' should map to EDUCATION."""
        result = standardize_emp_title(sample_customer_df)
        cat = result.filter(F.col("member_id") == "m003").select("emp_category").collect()[0][0]
        assert cat == "EDUCATION"

    def test_null_title(self, spark, sample_customer_df):
        """Null emp_title should map to UNKNOWN."""
        result = standardize_emp_title(sample_customer_df)
        cat = result.filter(F.col("member_id") == "m004").select("emp_category").collect()[0][0]
        assert cat == "UNKNOWN"


class TestValidateState:
    """Tests for state code validation and region enrichment."""

    def test_valid_state(self, spark, sample_customer_df):
        """'CA' is a valid state code."""
        result = validate_and_enrich_state(sample_customer_df)
        state = result.filter(F.col("member_id") == "m001").select("address_state").collect()[0][0]
        assert state == "CA"

    def test_invalid_state_replaced(self, spark, sample_customer_df):
        """'XY' is invalid and should be replaced with 'NA'."""
        result = validate_and_enrich_state(sample_customer_df)
        state = result.filter(F.col("member_id") == "m004").select("address_state").collect()[0][0]
        assert state == "NA"

    def test_region_mapping(self, spark, sample_customer_df):
        """'CA' should map to 'West' region."""
        result = validate_and_enrich_state(sample_customer_df)
        region = result.filter(F.col("member_id") == "m001").select("address_region").collect()[0][0]
        assert region == "West"


class TestCapIncomeOutliers:
    """Tests for income outlier capping."""

    def test_normal_income_unchanged(self, spark, sample_customer_df):
        """Income within range should remain unchanged."""
        result = cap_income_outliers(sample_customer_df)
        income = result.filter(F.col("member_id") == "m001").select("annual_income").collect()[0][0]
        assert income == 120000.0

    def test_outlier_flag_false_for_normal(self, spark, sample_customer_df):
        """Normal income should not be flagged."""
        result = cap_income_outliers(sample_customer_df)
        flag = result.filter(F.col("member_id") == "m001").select("income_outlier_flag").collect()[0][0]
        assert flag is False


class TestDerivedFeatures:
    """Tests for derived feature columns."""

    def test_income_bracket_middle(self, spark, sample_customer_df):
        """$55,000 income should be 'Lower-Middle' bracket."""
        result = add_derived_features(sample_customer_df)
        bracket = result.filter(F.col("member_id") == "m002").select("income_bracket").collect()[0][0]
        assert bracket == "Lower-Middle"

    def test_income_bracket_upper_middle(self, spark, sample_customer_df):
        """$120,000 income should be 'Upper-Middle' bracket."""
        result = add_derived_features(sample_customer_df)
        bracket = result.filter(F.col("member_id") == "m001").select("income_bracket").collect()[0][0]
        assert bracket == "Upper-Middle"

    def test_credit_utilization_ratio(self, spark, sample_customer_df):
        """Credit utilization should be income / credit_limit."""
        result = add_derived_features(sample_customer_df)
        ratio = result.filter(F.col("member_id") == "m001").select("credit_utilization_ratio").collect()[0][0]
        assert ratio == pytest.approx(120000.0 / 300000.0, rel=0.01)
