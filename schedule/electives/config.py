import json
from pathlib import Path

from pydantic import parse_obj_as

from config_base import BaseParserConfig
from schedule.electives.models import Elective
from utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"
"""Path to config.json file"""


class ElectivesParserConfig(BaseParserConfig):
    """
    Config for electives parser from Google Sheets
    """

    ELECTIVES: list[Elective]
    """Electives list"""


with open(CONFIG_PATH, "r") as f:
    elective_config_dict = json.load(f)

electives_config: ElectivesParserConfig = parse_obj_as(
    ElectivesParserConfig, elective_config_dict
)


if __name__ == "__main__":
    cfg = BaseParserConfig(
        SAVE_ICS_PATH=Path(""),
        SAVE_JSON_PATH=Path(""),
    )
    print(PROJECT_ROOT)
    print(cfg.CREDENTIALS_PATH.absolute())
