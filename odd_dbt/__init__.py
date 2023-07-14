from odd_dbt.logger import logger


def get_version() -> str:
    try:
        from odd_dbt.__version__ import VERSION
        return VERSION
    except ImportError:
        logger.warning("Can't get version from odd_dbt.__version__")
        return "0.0.0"