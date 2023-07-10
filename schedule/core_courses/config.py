import json
from pathlib import Path

from pydantic import parse_obj_as

from schedule.config_base import BaseParserConfig
from schedule.utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"
"""Path to config.json file"""


class CoreCoursesParserConfig(BaseParserConfig):
    """
    Config for core courses parser from Google Sheets
    """

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
