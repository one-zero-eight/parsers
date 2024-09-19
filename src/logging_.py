__all__ = ["logger"]

import logging.config
import os

import yaml


class RelativePathFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.relativePath = os.path.relpath(record.pathname)
        return True


with open("logging.yaml", "r") as f:
    config = yaml.safe_load(f)
    logging.config.dictConfig(config)

logger = logging.getLogger("src")
logger.addFilter(RelativePathFilter())
