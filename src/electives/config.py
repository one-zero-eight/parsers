from pathlib import Path

from pydantic import BaseModel, field_validator

from src.config_base import BaseParserConfig
from src.electives.models import Elective
from src.utils import get_project_root

PROJECT_ROOT = get_project_root()

config_path = Path(__file__).parent / "config.yaml"


class Target(BaseModel):
    sheet_name: str


class Tag(BaseModel):
    alias: str
    type: str
    name: str


class ElectivesParserConfig(BaseParserConfig):
    targets: list[Target]

    semester_tag: Tag

    spreadsheet_id: str
    distribution_spreadsheet_id: str | None = None
    temp_dir: Path = PROJECT_ROOT / "temp" / "electives"

    electives: list["Elective"]

    @field_validator("temp_dir", mode="before")
    @classmethod
    def ensure_dir(cls, v):
        """Ensure that directory exists"""
        v.mkdir(parents=True, exist_ok=True)
        return v


electives_config: ElectivesParserConfig = ElectivesParserConfig.from_yaml(config_path)
