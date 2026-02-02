# Elasticsearch-to-AWS Data Lake Pipeline

ETL pipeline that extracts data from Elasticsearch and loads it into AWS Data Lake (S3, Glue, Athena, Redshift).

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
┌─────────────────┐     ┌─────────────┐     ┌─────────────┐
│  Elasticsearch  │────▶│   Airflow   │────▶│     S3      │
│    (Docker)     │     │    ETL      │     │  (Raw Data) │
└─────────────────┘     └─────────────┘     └──────┬──────┘
                                                   │
                                                   ▼
                                           ┌─────────────┐
                                           │    Glue     │
                                           │   Crawler   │
                                           └──────┬──────┘
                                                  │
                              ┌───────────────────┴───────────────────┐
                              ▼                                       ▼
                       ┌─────────────┐                         ┌─────────────┐
                       │   Athena    │                         │  Redshift   │
                       │  (Ad-hoc)   │                         │    (DW)     │
                       └─────────────┘                         └─────────────┘
```

## Key Features

- **REST API** (FastAPI) for triggering pipelines, querying status, and listing S3 data
- **Scroll API** for efficient large-dataset extraction from Elasticsearch
- **Parquet output** with Snappy compression for optimized data lake storage
- **Data quality validation** (schema, row count, duplicate, null checks)
- **Custom exception hierarchy** for granular error handling and resource cleanup
- **IAM role support** with environment variable fallback for AWS credentials
- **Idempotent DAG** using Airflow execution date for deterministic file naming
- **Comprehensive test suite** with 86 unit and integration tests

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
│   ├── elasticsearch_etl.py     # ES extraction, transform, CSV/Parquet
│   └── aws_etl.py               # S3 upload with pagination and IAM support
├── pipelines/
│   ├── elasticsearch_pipeline.py  # ES ETL orchestration with validation
│   └── aws_s3_pipeline.py        # S3 upload orchestration
├── utils/
│   ├── constants.py             # Config loading (env var + file fallback)
│   ├── exceptions.py            # Custom exception classes
│   └── validators.py            # Data quality validation
└── tests/
    ├── conftest.py              # Shared pytest fixtures
    ├── test_api.py              # REST API tests (28 tests)
    ├── test_elasticsearch_etl.py
    ├── test_aws_etl.py
    ├── test_validators.py
    └── test_pipelines.py        # Pipeline integration tests
```

## Prerequisites

- Docker & Docker Compose
- AWS Account with S3, Glue, Athena, Redshift access
- Python 3.10+

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/TonyTang222/Elasticsearch-to-AWS-DataLake-Pipeline.git
cd Elasticsearch-to-AWS-DataLake-Pipeline
```

### 2. Create virtual environment and install dependencies

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure credentials

Copy the example config and edit it:

```bash
cp config/config.conf.example config/config.conf
```

Edit `config/config.conf` with your AWS credentials. Alternatively, set environment variables (recommended for production):

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1
export AWS_BUCKET_NAME=your-elasticsearch-datalake-bucket
```

### 4. Start services

```bash
# Start Elasticsearch, Kibana, and Airflow
docker compose up -d

# Check service health
docker compose ps
```

### 5. Access the services

| Service | URL | Credentials |
|---------|-----|-------------|
| FastAPI (Swagger) | http://localhost:8000/docs | - |
| Kibana | http://localhost:5601 | - |
| Airflow | http://localhost:8080 | airflow / airflow |
| Elasticsearch | http://localhost:9200 | - |

### 6. Insert test data into Elasticsearch

Using Kibana Dev Tools or curl:

```bash
curl -X POST "localhost:9200/logs/_doc" -H 'Content-Type: application/json' -d'
{
  "@timestamp": "2026-01-28T10:00:00Z",
  "level": "INFO",
  "message": "Test log message",
  "service": "api-gateway"
}'
```

### 7. Start the API server (local development)

```bash
ES_HOST=localhost uvicorn api.main:app --reload
```

### 8. Trigger the pipeline

**Via API:**

```bash
curl -X POST http://localhost:8000/api/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{"index_name": "logs", "output_format": "parquet"}'
```

**Via Airflow UI:**

1. Go to http://localhost:8080
2. Enable the `elasticsearch_etl_dag`
3. Trigger the DAG manually

## REST API Endpoints

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| GET | `/health` | Health check + ES connectivity | 200 |
| POST | `/api/v1/pipelines/run` | Trigger a pipeline run (async) | 202 |
| GET | `/api/v1/pipelines/runs` | List all pipeline runs | 200 |
| GET | `/api/v1/pipelines/runs/{run_id}` | Get run status and details | 200 |
| GET | `/api/v1/data/files` | List files in S3 bucket | 200 |
| GET | `/api/v1/data/preview` | Preview Parquet file contents as JSON | 200 |

Interactive API documentation available at `http://localhost:8000/docs` (Swagger UI).

## S3 Output Structure

Data is stored in Parquet format with date-based partitioning:

```
s3://your-bucket/
└── raw/
    └── elasticsearch/
        └── 2025-01-01/
            └── elasticsearch_20250101.parquet
```

## Running Tests

```bash
pytest tests/ -v
```

## Stopping Services

```bash
docker compose down

# To remove all data volumes
docker compose down -v
```
