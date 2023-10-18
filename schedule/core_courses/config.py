import datetime
import json
from pathlib import Path

from pydantic import parse_obj_as, validator, BaseModel

from schedule.config_base import BaseParserConfig
from schedule.utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"
"""Path to config.json file"""


class CoreCoursesConfig(BaseParserConfig):
    """
    Config for electives parser from Google Sheets
    """

    class Target(BaseModel):
        """
        Target model
        """

        sheet_name: str
        """Sheet name"""
        range: str
        """Range"""
        time_columns: list[str]
        """Time columns"""
        start_date: datetime.date
        """Datetime start"""
        end_date: datetime.date
        """Datetime end"""

    class Tag(BaseModel):
        """
        Tag model
        """

        alias: str
        """Slugged alias of tag"""
        type: str
        """Type"""
        name: str
        """Short name"""

    TARGETS: list[Target]
    """List of targets"""
    SEMESTER_TAG: Tag
    """Semester tag"""

    SPREADSHEET_ID: str
    TEMP_DIR: Path = PROJECT_ROOT / "temp" / "core-courses"
    WEEKDAYS = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ]
    ICS_WEEKDAYS = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]
    IGNORED_SUBJECTS: list[str] = ["Elective courses on Physical Education"]

    @validator("TEMP_DIR", pre=True)
    def ensure_dir(cls, v):
        """Ensure that directory exists"""
        v.mkdir(parents=True, exist_ok=True)
        return v


with open(CONFIG_PATH, "r") as f:
    elective_config_dict = json.load(f)

core_courses_config: CoreCoursesConfig = parse_obj_as(
    CoreCoursesConfig, elective_config_dict
)
