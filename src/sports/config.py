from pydantic import ConfigDict

__all__ = ["sports_config", "SportsParserConfig"]

import datetime
from pathlib import Path

from pydantic import BaseModel, SecretStr

from src.config_base import BaseParserConfig

config_path = Path(__file__).parent / "config.yaml"


class Credentials(BaseModel):
    email: str
    password: SecretStr


class Token(BaseModel):
    token: SecretStr


class SportsParserConfig(BaseParserConfig):
    start_of_semester: datetime.date
    end_of_semester: datetime.date

    website_url: str = "https://sport.innopolis.university"
    api_url: str = "https://sport.innopolis.university/api"
    model_config = ConfigDict(validate_assignment=True)


sports_config: SportsParserConfig = SportsParserConfig.from_yaml(config_path)
