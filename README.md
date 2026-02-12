# Elasticsearch-to-AWS Data Lake Pipeline

ETL pipeline that extracts data from Elasticsearch and loads it into an AWS Lakehouse architecture with Apache Iceberg tables on S3.

## Architecture

```
                                ┌─────────────┐
                                │  FastAPI    │
                                │   :8000     │
                                └──────┬──────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
              POST /run         GET /runs/{id}     GET /data/files
                    │                  │                  │
                    ▼                  ▼                  ▼
┌─────────────────┐     ┌─────────────┐     ┌──────────────────┐
│  Elasticsearch  │────▶│   Airflow   │────▶│  S3 Bronze Layer │
│    (Docker)     │     │    ETL      │     │  (Raw Parquet)   │
└─────────────────┘     └─────────────┘     └────────┬─────────┘
                                                     │
                                                     ▼
                                            ┌──────────────────┐
                                            │  S3 Silver Layer │
                                            │ (Iceberg Tables) │
                                            └────────┬─────────┘
                                                     │
                                              Glue Catalog
                                                     │
                                                     ▼
                                              ┌─────────────┐
                                              │   Athena    │
                                              │  (Query)    │
                                              └─────────────┘
```

---

## Key Features

- **Lakehouse architecture** with Bronze (raw Parquet) and Silver (Iceberg) layers on S3
- **Apache Iceberg tables** with schema evolution, snapshot history, and partition management via PyIceberg
- **REST API** (FastAPI) for triggering pipelines, querying status, and listing S3 data
- **Scroll API** for efficient large-dataset extraction from Elasticsearch
- **Parquet output** with Snappy compression for optimized data lake storage
- **Data quality validation** (schema, row count, duplicate, null checks)
- **Custom exception hierarchy** for granular error handling and resource cleanup
- **IAM role support** with environment variable fallback for AWS credentials
- **Idempotent DAG** using Airflow execution date for deterministic file naming
- **Comprehensive test suite** with 100+ unit and integration tests

---

## Lakehouse Architecture

The pipeline implements a two-layer lakehouse architecture on S3:

**Bronze Layer** (Raw Parquet) - Immutable landing zone for extracted data:
- Date-partitioned Parquet files written directly to S3
- Preserves raw data as-is from Elasticsearch

**Silver Layer** (Iceberg Tables) - Curated, schema-managed data:
- Apache Iceberg table format managed by PyIceberg
- AWS Glue Catalog for metadata management
- Schema evolution without data rewrites
- Snapshot history for time-travel queries
- Partition evolution (default: `day(extracted_at)`)

### S3 Output Structure

```
s3://your-bucket/
├── bronze/                          # Bronze Layer (raw Parquet)
│   └── elasticsearch/
│       └── 2025-01-01/
│           └── elasticsearch_20250101.parquet
└── silver/                          # Silver Layer (Iceberg tables)
    └── datalake/
        └── elasticsearch_logs/
            ├── metadata/            # Iceberg metadata (snapshots, schema)
            └── data/                # Iceberg-managed Parquet files
```

---

## REST API Endpoints

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| GET | `/health` | Health check + ES connectivity | 200 |
| POST | `/api/v1/pipelines/run` | Trigger a pipeline run (async) | 202 |
| GET | `/api/v1/pipelines/runs` | List all pipeline runs | 200 |
| GET | `/api/v1/pipelines/runs/{run_id}` | Get run status and details | 200 |
| GET | `/api/v1/data/files` | List files in S3 bucket | 200 |
| GET | `/api/v1/data/preview` | Preview Parquet file contents as JSON | 200 |
| GET | `/api/v1/data/iceberg/preview` | Query Iceberg Silver layer (time-travel) | 200 |
| GET | `/api/v1/data/iceberg/snapshots` | List Iceberg snapshot history | 200 |

Interactive API documentation available at `http://localhost:8000/docs` (Swagger UI).

---

## Quick Start

**Prerequisites**: Docker, Python 3.10+, AWS Account with S3, Glue, Athena access

```bash
# Clone the repository
git clone https://github.com/TonyTang222/Elasticsearch-to-AWS-DataLake-Pipeline.git
cd Elasticsearch-to-AWS-DataLake-Pipeline

# Set up Python environment
python -m venv venv
source venv/bin/activate  # macOS/Linux
pip install -r requirements.txt

# Configure credentials
cp config/config.conf.example config/config.conf

# Start Elasticsearch, Kibana, and Airflow
docker compose up -d
```

### Configure credentials

Edit `config/config.conf` with your AWS credentials. Alternatively, set environment variables (recommended for production):

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1
export AWS_BUCKET_NAME=your-elasticsearch-datalake-bucket

# Iceberg / Lakehouse (optional)
export ICEBERG_CATALOG_TYPE=glue
export ICEBERG_NAMESPACE=datalake
export ICEBERG_TABLE_NAME=elasticsearch_logs
export ICEBERG_WAREHOUSE_PATH=s3://your-bucket/silver
```

### Load sample data into Elasticsearch

1. Go to Kibana at http://localhost:5601
2. Navigate to Home → "Try sample data"
3. Click "Add data" on "Sample web logs"

### Trigger the pipeline

**Via API:**

```bash
ES_HOST=localhost uvicorn api.main:app --reload

curl -X POST http://localhost:8000/api/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{"index_name": "kibana_sample_data_logs"}'
```

**Via Airflow UI:**

1. Go to http://localhost:8080 (airflow / airflow)
2. Enable the `elasticsearch_etl_dag`
3. Trigger the DAG manually

### Running Tests

```bash
pytest tests/ -v
```

### Stopping Services

```bash
docker compose down

# To remove all data volumes
docker compose down -v
```

---

## Project Structure

```
Elasticsearch-to-AWS-DataLake-Pipeline/
├── README.md
├── requirements.txt
├── docker-compose.yml           # Elasticsearch + Kibana + Airflow + FastAPI
├── config/
│   └── config.conf.example      # Configuration template
├── api/
│   ├── main.py                  # FastAPI app, lifespan, exception handlers
│   ├── models.py                # Pydantic request/response models
│   ├── run_store.py             # Thread-safe in-memory run tracking
│   ├── dependencies.py          # FastAPI dependency injection
│   └── routes/
│       ├── health.py            # GET /health
│       ├── pipelines.py         # Pipeline trigger and status endpoints
│       └── data.py              # S3 data query endpoints
├── dags/
│   └── elasticsearch_dag.py     # Airflow DAG with failure callbacks
├── etls/
│   ├── elasticsearch_etl.py     # ES extraction, transform, Parquet output
│   ├── aws_etl.py               # S3 upload with pagination and IAM support
│   └── iceberg_etl.py           # Iceberg table operations (PyIceberg)
├── pipelines/
│   ├── elasticsearch_pipeline.py  # ES ETL orchestration with validation
│   ├── aws_s3_pipeline.py        # S3 upload orchestration (Bronze layer)
│   └── iceberg_pipeline.py       # Iceberg write orchestration (Silver layer)
├── utils/
│   ├── constants.py             # Config loading (env var + file fallback)
│   ├── exceptions.py            # Custom exception classes
│   └── validators.py            # Data quality validation
└── tests/
    ├── conftest.py              # Shared pytest fixtures
    ├── test_api.py              # REST API tests
    ├── test_elasticsearch_etl.py
    ├── test_aws_etl.py
    ├── test_iceberg_etl.py      # Iceberg ETL and pipeline tests
    ├── test_validators.py
    └── test_pipelines.py        # Pipeline integration tests
```

---

## Tech Stack

- **Python 3.10+** — Core language
- **Elasticsearch 8** — Source data store (with Scroll API for large-dataset extraction)
- **Apache Airflow** — DAG scheduling and orchestration
- **FastAPI** — REST API with async pipeline triggering
- **Apache Iceberg** — Table format via PyIceberg (schema evolution, time-travel)
- **AWS S3** — Data lake storage (Bronze + Silver layers)
- **AWS Glue Catalog** — Iceberg metadata management
- **AWS Athena** — Serverless SQL queries on Iceberg tables
- **Docker** — Containerized local development
- **pytest** — 100+ unit and integration tests
- **GitHub Actions** — CI/CD pipeline
