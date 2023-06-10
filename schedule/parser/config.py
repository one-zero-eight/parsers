from datetime import datetime
from pprint import pprint
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, validator
from pydantic.tools import parse_obj_as
from pydantic.dataclasses import dataclass
import json

from schedule.parser.utils import remove_trailing_spaces, symbol_translation

PARSER_PATH = Path(__file__).parent
CONFIG_PATH = PARSER_PATH / "config.json"


@dataclass
class AcademicParserConfig:
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
    CURRENT_YEAR = datetime.now().year


class Elective(BaseModel):
    alias: str
    name: Optional[str]
    instructor: Optional[str]
    type: Optional[str]

    @validator('name', 'instructor', 'type', pre=True, always=True)
    def beatify_string(cls, v):
        if v:
            v = remove_trailing_spaces(v)
            v = v.translate(symbol_translation)
        return v


@dataclass
class ElectivesParserConfig:
    SPREADSHEET_ID: str
    TARGET_RANGES: list[str]
    TARGET_SHEET_TITLES: list[str]
    SAVE_PATH: str
    SAVE_JSON_PATH: str

    ELECTIVES: list[Elective]

    CREDENTIALS_PATH: str = "credentials.json"

    API_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    TIMEZONE = 'Europe/Moscow'
    TIMEZONE_DELTA = '+03:00'

    DAYS = ['Monday',
            'Tuesday',
            'Wednesday',
            'Thursday',
            'Friday',
            'Saturday',
            'Sunday']


with open(CONFIG_PATH, 'r') as f:
    config_dict = json.load(f)
    academic_config_dict = config_dict["academic"]
    sport_config_dict = config_dict["sport"]
    elective_config_dict = config_dict["electives"]
    dormitory_config_dict = config_dict["dormitory"]

academic_config: AcademicParserConfig = parse_obj_as(
    AcademicParserConfig,
    academic_config_dict
)

electives_config: ElectivesParserConfig = parse_obj_as(
    ElectivesParserConfig,
    elective_config_dict
)

__all__ = ['academic_config',
           'electives_config',
           'Elective',
           'PARSER_PATH']

if __name__ == '__main__':
    pprint(academic_config)
    pprint(electives_config)
