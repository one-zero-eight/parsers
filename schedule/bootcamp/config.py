import json
from pathlib import Path

from pydantic import parse_obj_as, validator

from schedule.config_base import BaseParserConfig
from schedule.utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"
"""Path to config.json file"""


class BootcampParserConfig(BaseParserConfig):
    SPREADSHEET_PATH: Path
    YEAR_OF_BOOTCAMP: int = 2023

    @validator(
        "SPREADSHEET_PATH",
        pre=True,
        always=True,
    )
    def relative_path(cls, v):
        return Path(v)


with open(CONFIG_PATH, "r") as f:
    bootcamp_config_dict = json.load(f)

bootcamp_config: BootcampParserConfig = parse_obj_as(
    BootcampParserConfig, bootcamp_config_dict
)
