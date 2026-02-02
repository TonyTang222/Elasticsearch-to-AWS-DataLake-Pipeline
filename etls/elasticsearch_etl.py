import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from elasticsearch import Elasticsearch

from utils.exceptions import ElasticsearchConnectionError, ElasticsearchExtractionError

logger = logging.getLogger(__name__)


def connect_elasticsearch(host: str, port: int, username: str = None, password: str = None) -> Elasticsearch:
    """
    Connect to Elasticsearch cluster.

    Args:
        host: Elasticsearch host
        port: Elasticsearch port
        username: Optional username for authentication
        password: Optional password for authentication

    Returns:
        Elasticsearch client instance

    Raises:
        ElasticsearchConnectionError: If connection fails or ping is unsuccessful
    """
    try:
        if username and password:
            client = Elasticsearch(
                hosts=[{'host': host, 'port': port, 'scheme': 'http'}],
                basic_auth=(username, password)
            )
        else:
            client = Elasticsearch(
                hosts=[{'host': host, 'port': port, 'scheme': 'http'}]
            )

        # Test connection
        if client.ping():
            logger.info(f"Connected to Elasticsearch at {host}:{port}")
            return client
        else:
            raise ElasticsearchConnectionError(
                f"Failed to ping Elasticsearch at {host}:{port}"
            )

    except ElasticsearchConnectionError:
        raise
    except Exception as e:
        raise ElasticsearchConnectionError(
            f"Failed to connect to Elasticsearch at {host}:{port}: {e}"
        ) from e


def extract_documents(
    client: Elasticsearch,
    index: str,
    query: dict = None,
    size: int = 1000,
    scroll: str = '2m'
) -> list:
    """
    Extract documents from Elasticsearch index using scroll API for large datasets.

    Args:
        client: Elasticsearch client
        index: Index name to query
        query: Elasticsearch query DSL (default: match_all)
        size: Number of documents per scroll batch
        scroll: Scroll context timeout

    Returns:
        List of document dictionaries

    Raises:
        ElasticsearchExtractionError: If extraction fails
    """
    if query is None:
        query = {"match_all": {}}

    documents = []
    scroll_id = None

    try:
        # Initial search with scroll
        response = client.search(
            index=index,
            query=query,
            size=size,
            scroll=scroll
        )

        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']

        while len(hits) > 0:
            for hit in hits:
                doc = {
                    'id': hit['_id'],
                    'index': hit['_index'],
                    'score': hit.get('_score'),
                    **hit['_source']
                }
                documents.append(doc)

            # Get next batch
            response = client.scroll(scroll_id=scroll_id, scroll=scroll)
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

        logger.info(f"Extracted {len(documents)} documents from index '{index}'")
        return documents

    except Exception as e:
        raise ElasticsearchExtractionError(
            f"Failed to extract documents from index '{index}': {e}"
        ) from e

    finally:
        # Always clean up scroll context to prevent resource leaks
        if scroll_id:
            try:
                client.clear_scroll(scroll_id=scroll_id)
            except Exception as cleanup_error:
                logger.warning(f"Failed to clear scroll context: {cleanup_error}")


def extract_documents_by_time_range(
    client: Elasticsearch,
    index: str,
    timestamp_field: str = '@timestamp',
    start_time: datetime = None,
    end_time: datetime = None,
    size: int = 1000
) -> list:
    """
    Extract documents within a specific time range.

    Args:
        client: Elasticsearch client
        index: Index name to query
        timestamp_field: Name of the timestamp field
        start_time: Start of time range (default: 24 hours ago)
        end_time: End of time range (default: now)
        size: Number of documents per batch

    Returns:
        List of document dictionaries
    """
    if end_time is None:
        end_time = datetime.now(timezone.utc)
    if start_time is None:
        start_time = end_time - timedelta(hours=24)

    query = {
        "range": {
            timestamp_field: {
                "gte": start_time.isoformat(),
                "lte": end_time.isoformat()
            }
        }
    }

    return extract_documents(client, index, query, size)


def transform_data(docs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform extracted documents DataFrame.

    Args:
        docs_df: DataFrame with raw documents

    Returns:
        Transformed DataFrame
    """
    if docs_df.empty:
        logger.warning("Empty DataFrame received, nothing to transform")
        return docs_df

    # Convert timestamp fields if present
    timestamp_columns = ['@timestamp', 'timestamp', 'created_at', 'updated_at']
    for col in timestamp_columns:
        if col in docs_df.columns:
            docs_df[col] = pd.to_datetime(docs_df[col], errors='coerce')

    # Add extraction metadata
    docs_df['extracted_at'] = datetime.now(timezone.utc)

    # Handle nested JSON fields - flatten if needed
    for col in docs_df.columns:
        if docs_df[col].apply(lambda x: isinstance(x, dict)).any():
            # Convert dict columns to JSON strings for CSV compatibility
            docs_df[col] = docs_df[col].apply(
                lambda x: str(x) if isinstance(x, dict) else x
            )

    logger.info(f"Transformed DataFrame with {len(docs_df)} rows and {len(docs_df.columns)} columns")
    return docs_df


def load_data_to_csv(data: pd.DataFrame, path: str) -> str:
    """
    Save DataFrame to CSV file.

    Args:
        data: DataFrame to save
        path: Output file path

    Returns:
        Path to saved file
    """
    data.to_csv(path, index=False)
    logger.info(f"Data saved to {path}")
    return path


def load_data_to_parquet(data: pd.DataFrame, path: str, compression: str = 'snappy') -> str:
    """
    Save DataFrame to Parquet file with compression.

    Args:
        data: DataFrame to save
        path: Output file path
        compression: Compression codec ('snappy', 'gzip', 'brotli', None)

    Returns:
        Path to saved file
    """
    data.to_parquet(path, index=False, compression=compression, engine='pyarrow')
    logger.info(f"Data saved to Parquet: {path} (compression={compression})")
    return path
