from prefect.logging import get_run_logger
import logging

logger = None

try:
    logger = get_run_logger()
except Exception:
    logger = logging.getLogger(__name__)
