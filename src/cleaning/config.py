import datetime
import json
from pathlib import Path

from pydantic import BaseModel, parse_obj_as

from src.config_base import BaseParserConfig
from src.utils import get_project_root

PROJECT_ROOT = get_project_root()
CONFIG_PATH = Path(__file__).parent / "config.json"


class LinenChangeEntry(BaseModel):
    name: str = "Linen change"
    location: str
    rrule: dict[str, str]


class CleaningParserConfig(BaseParserConfig):
    start_date: datetime.date
    cleaning_spreadsheet_url: str
    linen_change_entries: list[LinenChangeEntry]


with open(CONFIG_PATH, "r") as f:
    elective_config_dict = json.load(f)

core_courses_config: CleaningParserConfig = parse_obj_as(CleaningParserConfig, elective_config_dict)
