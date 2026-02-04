import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), "../config/config.conf"))


def get_config(section: str, key: str, env_var: str = None, default: str = None) -> str:
    """
    Get configuration value with fallback chain:
    1. Environment variable (if env_var specified)
    2. Config file value
    3. Default value

    Args:
        section: Config file section name
        key: Config file key name
        env_var: Environment variable name to check first
        default: Default value if not found elsewhere

    Returns:
        Configuration value

    Raises:
        KeyError: If value not found and no default provided
    """
    if env_var:
        value = os.environ.get(env_var)
        if value:
            return value

    try:
        return parser.get(section, key)
    except (configparser.NoSectionError, configparser.NoOptionError):
        if default is not None:
            return default
        raise


# Elasticsearch
ES_HOST = get_config("elasticsearch", "elasticsearch_host", "ES_HOST", "localhost")
ES_PORT = get_config("elasticsearch", "elasticsearch_port", "ES_PORT", "9200")
ES_INDEX = get_config("elasticsearch", "elasticsearch_index", "ES_INDEX", "logs")
ES_USERNAME = get_config("elasticsearch", "elasticsearch_username", "ES_USERNAME", "")
ES_PASSWORD = get_config("elasticsearch", "elasticsearch_password", "ES_PASSWORD", "")

# AWS - environment variables and IAM roles are preferred over config file
AWS_ACCESS_KEY_ID = get_config("aws", "aws_access_key_id", "AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = get_config("aws", "aws_secret_access_key", "AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = get_config("aws", "aws_region", "AWS_REGION", "us-east-1")
AWS_BUCKET_NAME = get_config("aws", "aws_bucket_name", "AWS_BUCKET_NAME", "")

# API
API_HOST = get_config("api", "api_host", "API_HOST", "0.0.0.0")
API_PORT = get_config("api", "api_port", "API_PORT", "8000")

# File Paths
INPUT_PATH = get_config("file_paths", "input_path", "INPUT_PATH", "/opt/airflow/data/input")
OUTPUT_PATH = get_config("file_paths", "output_path", "OUTPUT_PATH", "/opt/airflow/data/output")

# Iceberg / Lakehouse
ICEBERG_CATALOG_NAME = get_config("iceberg", "catalog_name", "ICEBERG_CATALOG_NAME", "glue")
ICEBERG_CATALOG_TYPE = get_config("iceberg", "catalog_type", "ICEBERG_CATALOG_TYPE", "glue")
ICEBERG_NAMESPACE = get_config("iceberg", "namespace", "ICEBERG_NAMESPACE", "datalake")
ICEBERG_TABLE_NAME = get_config("iceberg", "table_name", "ICEBERG_TABLE_NAME", "elasticsearch_logs")
ICEBERG_WAREHOUSE_PATH = get_config("iceberg", "warehouse_path", "ICEBERG_WAREHOUSE_PATH", "s3://bucket/silver")
