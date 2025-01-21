import json
from pathlib import Path

from pydantic import BaseModel, parse_obj_as, validator

from src.config_base import BaseParserConfig
from src.electives.models import Elective
from src.utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"
"""Path to config.json file"""


class ElectivesParserConfig(BaseParserConfig):
    """
    Config for electives parser from Google Sheets
    """

    class Target(BaseModel):
        """
        Target model for electives (sheet in Google Sheets)
        """

        sheet_name: str
        range: str

    TARGETS: list[Target]

    class Tag(BaseModel):
        alias: str
        type: str
        name: str

    SEMESTER_TAG: Tag

    SPREADSHEET_ID: str
    TEMP_DIR: Path = PROJECT_ROOT / "temp" / "electives"

    ELECTIVES: list["Elective"]

    @validator("TEMP_DIR", pre=True)
    def ensure_dir(cls, v):
        """Ensure that directory exists"""
        v.mkdir(parents=True, exist_ok=True)
        return v


with open(CONFIG_PATH, "r") as f:
    elective_config_dict = json.load(f)

electives_config: ElectivesParserConfig = parse_obj_as(ElectivesParserConfig, elective_config_dict)
