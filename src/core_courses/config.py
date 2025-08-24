import datetime
from pathlib import Path

from pydantic import BaseModel, field_validator

from src.config_base import BaseParserConfig
from src.utils import get_project_root

PROJECT_ROOT = get_project_root()

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
    temp_dir: Path = PROJECT_ROOT / "temp" / "core-courses"
    "Temp directory"
    ignored_subjects: list[str] = [
        "Elective courses on Physical Education",
        "Elective course on Physical Education",
    ]

    @field_validator("temp_dir", mode="before")
    @classmethod
    def ensure_dir(cls, v):
        "Ensure that directory exists"
        v.mkdir(parents=True, exist_ok=True)
        return v


core_courses_config: CoreCoursesConfig = CoreCoursesConfig.from_yaml(config_path)
