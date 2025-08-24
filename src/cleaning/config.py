import datetime
from pathlib import Path

from pydantic import BaseModel

from src.config_base import BaseParserConfig
from src.utils import get_project_root

PROJECT_ROOT = get_project_root()
config_path = Path(__file__).parent / "config.yaml"


class LinenChangeEntry(BaseModel):
    name: str = "Linen change"
    location: str
    rrule: dict[str, str]


class CleaningParserConfig(BaseParserConfig):
    start_date: datetime.date
    cleaning_spreadsheet_url: str
    linen_change_entries: list[LinenChangeEntry]

cleaning_config: CleaningParserConfig = CleaningParserConfig.from_yaml(config_path)
