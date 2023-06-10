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


class BaseParserConfig(BaseModel):
    """
    Base config for parsers
    """

    SPREADSHEET_ID: str
    """Spreadsheet ID from Google Sheets URL"""
    TARGET_RANGES: list[str]
    """Target ranges from spreadsheet"""
    TARGET_SHEET_TITLES: list[str]
    """Target sheet titles from spreadsheet"""
    SAVE_ICS_PATH: str
    """Path to save .ics files"""
    SAVE_JSON_PATH: str
    """Path to save .json file"""

    CREDENTIALS_PATH: str = "credentials.json"
    """Path to credentials.json file"""

    API_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    """API scopes for Google Sheets API"""
    TIMEZONE = "Europe/Moscow"
    """Timezone for events"""
    TIMEZONE_DELTA = "+03:00"
    """Timezone delta for events"""

    DAYS = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ]
    """Days of week"""


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

    @validator("name", "instructor", "elective_type", pre=True)
    def beatify_string(cls: type["Elective"], string: str) -> str:  # noqa
        """Beatify string

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


with open(CONFIG_PATH, "r") as f:
    config_dict = json.load(f)
    core_courses_config_dict = config_dict["core-courses"]
    sport_config_dict = config_dict["sport"]
    elective_config_dict = config_dict["electives"]
    dormitory_config_dict = config_dict["dormitory"]

core_courses_config: CoreCoursesParserConfig = parse_obj_as(
    CoreCoursesParserConfig, core_courses_config_dict
)

electives_config: ElectivesParserConfig = parse_obj_as(
    ElectivesParserConfig, elective_config_dict
)

__all__ = [
    "core_courses_config",
    "electives_config",
    "Elective",
    "PARSER_PATH",
    "ElectivesParserConfig",
    "CoreCoursesParserConfig",
]

if __name__ == "__main__":
    pprint(core_courses_config)
    pprint(electives_config)
