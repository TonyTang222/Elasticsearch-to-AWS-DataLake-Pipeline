from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import logging

logger = logging.getLogger(__name__)

# Add project paths for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.elasticsearch_pipeline import elasticsearch_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.iceberg_pipeline import iceberg_pipeline


def on_failure_callback(context):
    """Log detailed error information on task failure."""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"]
    exception = context.get("exception", "Unknown error")

    error_message = f"Task failed! DAG: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {exception}"
    logger.error(error_message)


default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": on_failure_callback,
}

dag = DAG(
    dag_id="elasticsearch_etl_dag",
    default_args=default_args,
    description="Lakehouse pipeline from Elasticsearch to S3 with Iceberg",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["elasticsearch", "etl", "s3", "data-lake", "iceberg", "lakehouse"],
    doc_md="""
    ## Elasticsearch to S3 Lakehouse Pipeline

    Extracts data from Elasticsearch, validates quality, loads to S3 Bronze layer,
    and writes to Iceberg Silver layer for lakehouse analytics.

    **Tasks:**
    1. `extract_elasticsearch` - Extract documents from ES with time-range filtering
    2. `upload_to_s3` - Upload Parquet to S3 Bronze layer with date partitioning
    3. `write_to_iceberg` - Write data to Iceberg table in Silver layer
    """,
)


def extract_elasticsearch_data(**kwargs):
    """Extract data from Elasticsearch using execution date for idempotent file naming."""
    ds = kwargs["ds"]
    ds_nodash = ds.replace("-", "")

    file_name = f"elasticsearch_{ds_nodash}"

    logger.info(f"Starting extraction for date: {ds}")

    output_path = elasticsearch_pipeline(
        file_name=file_name, time_range_hours=24, use_time_filter=True, validate=True
    )

    logger.info(f"Extraction complete: {output_path}")
    return output_path


def upload_to_s3_task(**kwargs):
    """Upload extracted data to S3 Bronze layer."""
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="extract_elasticsearch")

    if not file_path:
        raise ValueError("No file path received from extraction task")

    logger.info(f"Uploading {file_path} to S3 Bronze layer")

    s3_uri = upload_s3_pipeline(file_path=file_path)

    logger.info(f"Upload complete: {s3_uri}")
    return s3_uri


def write_to_iceberg_task(**kwargs):
    """Write extracted data to Iceberg table in Silver layer."""
    import pandas as pd

    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="extract_elasticsearch")

    if not file_path:
        raise ValueError("No file path received from extraction task")

    logger.info(f"Writing {file_path} to Iceberg Silver layer")

    df = pd.read_parquet(file_path)
    result = iceberg_pipeline(df=df)

    logger.info(f"Iceberg write complete: snapshot={result['snapshot_id']}, records={result['records_added']}")
    return result


# Task 1: Extract from Elasticsearch
extract_elasticsearch = PythonOperator(
    task_id="extract_elasticsearch",
    python_callable=extract_elasticsearch_data,
    dag=dag,
)

# Task 2: Upload to S3 Bronze layer
upload_s3 = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3_task,
    dag=dag,
)

# Task 3: Write to Iceberg Silver layer
write_to_iceberg = PythonOperator(
    task_id="write_to_iceberg",
    python_callable=write_to_iceberg_task,
    dag=dag,
)

# Define task dependencies
extract_elasticsearch >> upload_s3 >> write_to_iceberg
