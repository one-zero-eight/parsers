__all__ = ["sports_config", "SportsParserConfig"]

import datetime
from pathlib import Path

from pydantic import BaseModel, validator, SecretStr

from schedule.config_base import VeryBaseParserConfig
from schedule.utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"


class Credentials(BaseModel):
    email: str
    password: SecretStr


class Token(BaseModel):
    token: SecretStr


class SportsParserConfig(VeryBaseParserConfig):
    START_OF_SEMESTER: datetime.date
    END_OF_SEMESTER: datetime.date

    website_url: str = "https://sport.innopolis.university"
    api_url: str = "https://sport.innopolis.university/api"

    class Config:
        validate_assignment = True


sports_config: SportsParserConfig = SportsParserConfig.parse_file(CONFIG_PATH)
