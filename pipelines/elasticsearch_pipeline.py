import os
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from etls.elasticsearch_etl import (
    connect_elasticsearch,
    extract_documents,
    extract_documents_by_time_range,
    transform_data,
    load_data_to_csv,
    load_data_to_parquet
)
from utils.constants import ES_HOST, ES_PORT, ES_INDEX, ES_USERNAME, ES_PASSWORD, OUTPUT_PATH
from utils.validators import DataValidator

logger = logging.getLogger(__name__)


def elasticsearch_pipeline(
    file_name: str,
    index_name: str = None,
    query: dict = None,
    time_range_hours: int = 24,
    use_time_filter: bool = True,
    output_format: str = 'parquet',
    validate: bool = True
) -> str:
    """
    Main ETL pipeline for extracting data from Elasticsearch.

    Args:
        file_name: Output file name (without extension)
        index_name: Elasticsearch index to query (default from config)
        query: Custom Elasticsearch query (optional)
        time_range_hours: Hours to look back for time-based queries
        use_time_filter: Whether to filter by time range
        output_format: Output format - 'csv' or 'parquet' (default: parquet)
        validate: Whether to run data quality validation

    Returns:
        Path to the output file

    Raises:
        ValueError: If no documents are extracted
    """
    # Use config values if not specified
    if index_name is None:
        index_name = ES_INDEX

    logger.info(f"Starting Elasticsearch pipeline for index: {index_name}")

    # Connect to Elasticsearch
    client = connect_elasticsearch(
        host=ES_HOST,
        port=int(ES_PORT),
        username=ES_USERNAME if ES_USERNAME else None,
        password=ES_PASSWORD if ES_PASSWORD else None
    )

    # Extract documents
    if use_time_filter and query is None:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=time_range_hours)
        logger.info(f"Extracting documents from {start_time} to {end_time}")

        documents = extract_documents_by_time_range(
            client=client,
            index=index_name,
            start_time=start_time,
            end_time=end_time
        )
    else:
        documents = extract_documents(
            client=client,
            index=index_name,
            query=query
        )

    if not documents:
        raise ValueError(f"No documents extracted from index '{index_name}'")

    # Convert to DataFrame
    source_count = len(documents)
    docs_df = pd.DataFrame(documents)
    logger.info(f"Created DataFrame with {len(docs_df)} rows")

    # Transform data
    docs_df = transform_data(docs_df)

    # Validate data quality
    if validate:
        validator = DataValidator()
        validator.validate_row_count(source_count, len(docs_df))
        validator.validate_no_duplicates(docs_df, key_columns=['id'])

        quality_report = validator.get_data_quality_report(docs_df)
        logger.info(f"Data quality report: {quality_report}")

    # Ensure output directory exists
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Save output
    if output_format == 'parquet':
        output_file = f"{OUTPUT_PATH}/{file_name}.parquet"
        load_data_to_parquet(docs_df, output_file)
    else:
        output_file = f"{OUTPUT_PATH}/{file_name}.csv"
        load_data_to_csv(docs_df, output_file)

    logger.info(f"Pipeline completed. Output: {output_file}")
    return output_file


if __name__ == "__main__":
    # Test run
    file_name = f"elasticsearch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    result = elasticsearch_pipeline(file_name=file_name, use_time_filter=False)
    print(f"Output file: {result}")
