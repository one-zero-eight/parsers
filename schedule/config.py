from pprint import pprint
from pathlib import Path
from pydantic.tools import parse_obj_as
from pydantic.dataclasses import dataclass
import json

PARSER_PATH = Path(__file__).parent
CONFIG_PATH = PARSER_PATH / "config.json"


@dataclass
class ParserConfig:
    SPREADSHEET_ID: str
    TARGET_RANGES: list[str]
    TARGET_SHEET_TITLES: list[str]
    RECURRENCE: list[dict]
    SAVE_PATH: str

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


with open(CONFIG_PATH, 'r') as f:
    config_dict = json.load(f)

config: ParserConfig = parse_obj_as(ParserConfig, config_dict)

__all__ = ['config', 'PARSER_PATH']

if __name__ == '__main__':
    pprint(config)
