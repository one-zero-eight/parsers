__all__ = [
    "cleaning_config",
    "CleaningParserConfig",
    "CleaningEntry",
    "LinenChangeEntry",
]

import datetime
from pathlib import Path

from pydantic import BaseModel, validator, SecretStr

from schedule.config_base import BaseParserConfig
from schedule.utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"


class CleaningEntry(BaseModel):
    name: str = "Cleaning"
    location: str
    dates: list[datetime.date]


class LinenChangeEntry(BaseModel):
    name: str = "Linen change"
    location: str
    rrule: dict[str, str]


class CleaningParserConfig(BaseParserConfig):
    START_DATE: datetime.date
    END_DATE: datetime.date

    CLEANING_ENTRIES: list[CleaningEntry]
    LINEN_CHANGE_ENTRIES: list[LinenChangeEntry]


cleaning_config: CleaningParserConfig = CleaningParserConfig.parse_file(CONFIG_PATH)
