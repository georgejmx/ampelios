import logging
from dotenv import load_dotenv

load_dotenv()

for logger_name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
    logging.getLogger(logger_name).propagate = True
