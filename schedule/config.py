from pprint import pprint
from pathlib import Path
from pydantic.tools import parse_obj_as
from pydantic.dataclasses import dataclass
import json

PARSER_PATH = Path(__file__).parent
CONFIG_PATH = PARSER_PATH / "config.json"


@dataclass
class AcademicParserConfig:
    SPREADSHEET_ID: str
    TARGET_RANGES: list[str]
    TARGET_SHEET_TITLES: list[str]
    RECURRENCE: list[dict]
    SAVE_PATH: str

    OUTPUT_JSON_NAME: str = "academic.json"

    TARGET_ID: int = 0

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

    IGNORING_CLASSES = ["Physical Education"]


with open(CONFIG_PATH, 'r') as f:
    config_dict = json.load(f)
    academic_config_dict = config_dict["academic"]
    sport_config_dict = config_dict["sport"]
    elective_config_dict = config_dict["elective"]
    dormitory_config_dict = config_dict["dormitory"]

config: AcademicParserConfig = parse_obj_as(AcademicParserConfig, academic_config_dict)

__all__ = ['config', 'PARSER_PATH']

if __name__ == '__main__':
    pprint(config)
