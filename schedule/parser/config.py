from datetime import datetime
from pprint import pprint
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, validator
from pydantic.tools import parse_obj_as

import json

from schedule.parser.utils import remove_trailing_spaces, symbol_translation

PARSER_PATH = Path(__file__).parent
"""Path to parser directory"""

CONFIG_PATH = PARSER_PATH / "config.json"
"""Path to config.json file"""


class AcademicParserConfig(BaseModel):
    SPREADSHEET_ID: str
    TARGET_RANGES: list[str]
    TARGET_SHEET_TITLES: list[str]
    RECURRENCE: list[dict]
    SAVE_PATH: str
    SAVE_JSON_PATH: str

    CREDENTIALS_PATH: str = "credentials.json"

    API_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    TIMEZONE = 'Europe/Moscow'
    TIMEZONE_DELTA = '+03:00'

    DAYS = ['MONDAY',
            'TUESDAY',
            'WEDNESDAY',
            'THURSDAY',
            'FRIDAY',
            'SATURDAY',
            'SUNDAY']

    IGNORING_SUBJECTS = ["Elective courses on Physical Education"]
    """Subjects to ignore"""
    CURRENT_YEAR = datetime.now().year
    """Current year"""


class Elective(BaseModel):
    """
    Elective model for ElectivesParserConfig
    """

    alias: str
    """Alias for elective"""
    name: Optional[str]
    """Name of elective"""
    instructor: Optional[str]
    """Instructor of elective"""
    elective_type: Optional[str]
    """Type of elective"""

    @validator("name", "instructor", "type", pre=True, always=True)
    def beatify_string(cls: type["Elective"], string: str) -> str:  # noqa
        """
        Beatify string
        :param string: string to beatify
        :type string: str
        :return: beatified string
        :rtype: str
        """
        if string:
            string = remove_trailing_spaces(string)
            string = string.translate(symbol_translation)
        return string


class ElectivesParserConfig(BaseParserConfig):
    """
    Config for electives parser from Google Sheets
    """

    ELECTIVES: list[Elective]
    """Electives list""" ""

    CREDENTIALS_PATH: str = "credentials.json"
    """Path to credentials.json file"""

    DAYS = ['Monday',
            'Tuesday',
            'Wednesday',
            'Thursday',
            'Friday',
            'Saturday',
            'Sunday']

with open(CONFIG_PATH, "r") as f:
    config_dict = json.load(f)
    academic_config_dict = config_dict["academic"]
    sport_config_dict = config_dict["sport"]
    elective_config_dict = config_dict["electives"]
    dormitory_config_dict = config_dict["dormitory"]

academic_config: AcademicParserConfig = parse_obj_as(
    AcademicParserConfig, academic_config_dict
)

electives_config: ElectivesParserConfig = parse_obj_as(
    ElectivesParserConfig, elective_config_dict
)

__all__ = [
    "academic_config",
    "electives_config",
    "Elective",
    "PARSER_PATH",
    "ElectivesParserConfig",
    "AcademicParserConfig",
]

if __name__ == "__main__":
    pprint(academic_config)
    pprint(electives_config)
