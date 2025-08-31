from prefect.logging import get_run_logger
from prefect.exceptions import MissingContextError
import logging

_logger = None

def get_logger():
    global _logger
    if _logger is None:
        try:
            _logger = get_run_logger()
        except MissingContextError:
            _logger = logging.getLogger(__name__)
            logging.basicConfig(level=logging.INFO)
    return _logger

logger = get_logger()
