import logging
import os

logging.basicConfig(
        level=os.getenv("LOGLEVEL", "INFO"),
        format="[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
    )
logger = logging.getLogger("odd-dbt")
