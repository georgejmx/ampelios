import logging

from prefect.logging import get_run_logger

logger = None

try:
    logger = get_run_logger()
except Exception:
    logger = logging.getLogger(__name__)
