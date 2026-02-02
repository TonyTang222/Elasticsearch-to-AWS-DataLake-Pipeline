"""Data validation utilities for the pipeline."""

import logging
from typing import List, Dict, Any

import pandas as pd

from utils.exceptions import DataValidationError

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates data quality throughout the pipeline."""

    def validate_schema(
        self,
        df: pd.DataFrame,
        required_columns: List[str],
        raise_on_error: bool = True
    ) -> bool:
        """
        Validate that DataFrame contains required columns.

        Args:
            df: DataFrame to validate
            required_columns: List of column names that must be present
            raise_on_error: Whether to raise exception on failure

        Returns:
            True if validation passes

        Raises:
            DataValidationError: If required columns are missing
        """
        missing_columns = set(required_columns) - set(df.columns)

        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            if raise_on_error:
                raise DataValidationError(error_msg)
            return False

        logger.info(f"Schema validation passed. All {len(required_columns)} required columns present.")
        return True

    def validate_row_count(
        self,
        source_count: int,
        target_count: int,
        tolerance: float = 0.0,
        raise_on_error: bool = True
    ) -> bool:
        """
        Validate row counts match between source and target.

        Args:
            source_count: Number of rows in source
            target_count: Number of rows in target
            tolerance: Acceptable difference ratio (0.0 = exact match, 0.1 = 10% tolerance)
            raise_on_error: Whether to raise exception on failure

        Returns:
            True if validation passes

        Raises:
            DataValidationError: If row counts differ beyond tolerance
        """
        if source_count == 0:
            logger.warning("Source count is 0 - nothing to validate")
            return True

        diff_ratio = abs(source_count - target_count) / source_count

        if diff_ratio > tolerance:
            error_msg = (
                f"Row count mismatch: source={source_count}, "
                f"target={target_count}, diff={diff_ratio:.2%}"
            )
            logger.error(error_msg)
            if raise_on_error:
                raise DataValidationError(error_msg)
            return False

        logger.info(f"Row count validation passed: {target_count}/{source_count} rows")
        return True

    def validate_no_duplicates(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        raise_on_error: bool = True
    ) -> bool:
        """
        Validate no duplicate records based on key columns.

        Args:
            df: DataFrame to validate
            key_columns: Columns to check for duplicates
            raise_on_error: Whether to raise exception on failure

        Returns:
            True if no duplicates found

        Raises:
            DataValidationError: If duplicates are found
        """
        if not key_columns:
            return True

        existing_keys = [col for col in key_columns if col in df.columns]
        if not existing_keys:
            logger.warning(f"None of the key columns {key_columns} exist in DataFrame")
            return True

        duplicates = df.duplicated(subset=existing_keys, keep=False)
        duplicate_count = duplicates.sum()

        if duplicate_count > 0:
            error_msg = (
                f"Found {duplicate_count} duplicate records "
                f"based on columns: {existing_keys}"
            )
            logger.error(error_msg)
            if raise_on_error:
                raise DataValidationError(error_msg)
            return False

        logger.info(f"Duplicate validation passed: no duplicates found on {existing_keys}")
        return True

    def validate_not_null(
        self,
        df: pd.DataFrame,
        columns: List[str],
        raise_on_error: bool = True
    ) -> bool:
        """
        Validate that specified columns have no null values.

        Args:
            df: DataFrame to validate
            columns: Columns to check for nulls
            raise_on_error: Whether to raise exception on failure

        Returns:
            True if no nulls found

        Raises:
            DataValidationError: If null values are found
        """
        null_report = {}

        for col in columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    null_report[col] = int(null_count)

        if null_report:
            error_msg = f"Null values found: {null_report}"
            logger.error(error_msg)
            if raise_on_error:
                raise DataValidationError(error_msg)
            return False

        logger.info(f"Null validation passed for columns: {columns}")
        return True

    def get_data_quality_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate a data quality summary report for the DataFrame.

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary containing data quality metrics
        """
        report = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'null_counts': {k: int(v) for k, v in df.isnull().sum().to_dict().items()},
            'dtypes': df.dtypes.astype(str).to_dict(),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
        }

        if 'id' in df.columns:
            report['duplicate_ids'] = int(df['id'].duplicated().sum())

        logger.info(
            f"Data quality report: {report['row_count']} rows, "
            f"{report['column_count']} columns, "
            f"{report['memory_usage_mb']} MB"
        )
        return report
