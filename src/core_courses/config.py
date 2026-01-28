import datetime
from pathlib import Path

from pydantic import BaseModel

from src.config_base import BaseParserConfig

config_path = Path(__file__).parent / "config.yaml"


class Override(BaseModel):
    groups: list[str]
    "Groups"
    courses: list[str]
    "Courses"
    start_date: datetime.datetime
    "Datetime start"
    end_date: datetime.date
    "Datetime end"


class Target(BaseModel):
    sheet_name: str
    "Sheet name"
    start_date: datetime.date
    "Datetime start"
    end_date: datetime.date
    "Datetime end"
    override: list[Override]
    "Override"


class Tag(BaseModel):
    alias: str
    "Slugged alias of tag"
    type: str
    "Type"
    name: str
    "Short name"


class CoreCoursesConfig(BaseParserConfig):
    targets: list[Target]
    "List of targets"
    semester_tag: Tag
    "Semester tag"
    spreadsheet_id: str
    "Spreadsheet ID"
    ignored_subjects: list[str] = [
        "Elective courses on Physical Education",
        "Elective course on Physical Education",
    ]

core_courses_config: CoreCoursesConfig = CoreCoursesConfig.from_yaml(config_path)
