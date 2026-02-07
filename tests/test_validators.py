"""Tests for data validation utilities."""

import os
import sys

import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.validators import DataValidator
from utils.exceptions import DataValidationError


class TestValidateSchema:
    """Tests for schema validation."""

    @pytest.fixture
    def validator(self):
        return DataValidator()

    def test_schema_validation_passes(self, validator, sample_dataframe):
        result = validator.validate_schema(sample_dataframe, required_columns=["id", "message"])
        assert result is True

    def test_schema_validation_fails_on_missing_column(self, validator, sample_dataframe):
        with pytest.raises(DataValidationError, match="Missing required columns"):
            validator.validate_schema(sample_dataframe, required_columns=["id", "nonexistent_column"])


class TestValidateRowCount:
    """Tests for row count validation."""

    @pytest.fixture
    def validator(self):
        return DataValidator()

    def test_exact_match_passes(self, validator):
        result = validator.validate_row_count(100, 100)
        assert result is True

    def test_mismatch_fails(self, validator):
        with pytest.raises(DataValidationError, match="Row count mismatch"):
            validator.validate_row_count(100, 80)

    def test_within_tolerance_passes(self, validator):
        result = validator.validate_row_count(100, 95, tolerance=0.1)
        assert result is True

    def test_exceeds_tolerance_fails(self, validator):
        with pytest.raises(DataValidationError):
            validator.validate_row_count(100, 80, tolerance=0.1)

    def test_zero_source_returns_true(self, validator):
        result = validator.validate_row_count(0, 0)
        assert result is True


class TestValidateNoDuplicates:
    """Tests for duplicate validation."""

    @pytest.fixture
    def validator(self):
        return DataValidator()

    def test_no_duplicates_passes(self, validator, sample_dataframe):
        result = validator.validate_no_duplicates(sample_dataframe, key_columns=["id"])
        assert result is True

    def test_duplicates_detected(self, validator):
        df = pd.DataFrame({"id": ["1", "1", "2"], "value": ["a", "b", "c"]})
        with pytest.raises(DataValidationError, match="duplicate"):
            validator.validate_no_duplicates(df, key_columns=["id"])


class TestValidateNotNull:
    """Tests for null value validation."""

    @pytest.fixture
    def validator(self):
        return DataValidator()

    def test_no_nulls_passes(self, validator, sample_dataframe):
        result = validator.validate_not_null(sample_dataframe, columns=["id", "message"])
        assert result is True

    def test_nulls_detected(self, validator):
        df = pd.DataFrame({"id": ["1", None, "3"], "value": ["a", "b", "c"]})
        with pytest.raises(DataValidationError, match="Null values found"):
            validator.validate_not_null(df, columns=["id"])


class TestDataQualityReport:
    """Tests for data quality report generation."""

    @pytest.fixture
    def validator(self):
        return DataValidator()

    def test_report_contains_expected_fields(self, validator, sample_dataframe):
        report = validator.get_data_quality_report(sample_dataframe)

        assert report["row_count"] == 3
        assert report["column_count"] == 4
        assert "id" in report["columns"]
        assert "null_counts" in report
        assert "dtypes" in report
        assert "memory_usage_mb" in report

    def test_report_includes_duplicate_ids(self, validator):
        df = pd.DataFrame({"id": ["1", "1", "2"], "value": ["a", "b", "c"]})
        report = validator.get_data_quality_report(df)

        assert report["duplicate_ids"] == 1

    def test_report_without_id_column(self, validator):
        df = pd.DataFrame({"value": ["a", "b"]})
        report = validator.get_data_quality_report(df)

        assert "duplicate_ids" not in report
