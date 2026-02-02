"""Custom exceptions for the Elasticsearch-to-AWS Data Lake Pipeline."""


class PipelineError(Exception):
    """Base exception for all pipeline errors."""

    pass


class ElasticsearchConnectionError(PipelineError):
    """Raised when connection to Elasticsearch fails."""

    pass


class ElasticsearchExtractionError(PipelineError):
    """Raised when document extraction from Elasticsearch fails."""

    pass


class DataValidationError(PipelineError):
    """Raised when data validation checks fail."""

    pass


class S3UploadError(PipelineError):
    """Raised when uploading to S3 fails."""

    pass


class ConfigurationError(PipelineError):
    """Raised when configuration is invalid or missing."""

    pass
