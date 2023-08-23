import json
from pathlib import Path

from pydantic import parse_obj_as, Field

from schedule.config_base import GoogleSpreadsheetConfig
from schedule.utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"
"""Path to config.json file"""


class CoreCoursesParserConfig(GoogleSpreadsheetConfig):
    """
    Config for core courses parser from Google Sheets
    """

    SPREADSHEET_ID: str | None
    """Spreadsheet ID from Google Sheets URL"""
    TARGET_RANGES: list[str] = Field(default_factory=list)
    """Target ranges from spreadsheet"""
    TARGET_SHEET_TITLES: list[str] = Field(default_factory=list)
    """Target sheet titles from spreadsheet"""
    RECURRENCE: list[dict]
    """Recurrence for events
    For ex.:
    [{
        "desc": "From 23.01.2023 to 19.03.2023",
        "start": "20230123T000000",
        "end": "20230319T000000"
    }]
    """
    IGNORING_SUBJECTS = ["Elective courses on Physical Education"]
    """Subjects to ignore"""


with open(CONFIG_PATH, "r") as f:
    core_courses_config_dict = json.load(f)

core_courses_config: CoreCoursesParserConfig = parse_obj_as(
    CoreCoursesParserConfig, core_courses_config_dict
)

__all__ = [
    "core_courses_config",
    "CoreCoursesParserConfig",
]
