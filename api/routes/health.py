"""Health check endpoint."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter

from api.models import HealthResponse
from etls.elasticsearch_etl import connect_elasticsearch
from utils.constants import ES_HOST, ES_PORT, ES_USERNAME, ES_PASSWORD

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health_check():
    """Check API health and Elasticsearch connectivity."""
    es_status = "unavailable"
    try:
        connect_elasticsearch(
            host=ES_HOST,
            port=int(ES_PORT),
            username=ES_USERNAME if ES_USERNAME else None,
            password=ES_PASSWORD if ES_PASSWORD else None,
        )
        es_status = "connected"
    except Exception as e:
        logger.warning(f"Elasticsearch health check failed: {e}")

    return HealthResponse(
        status="healthy",
        version="1.0.0",
        elasticsearch=es_status,
        timestamp=datetime.now(timezone.utc),
    )
