from pathlib import Path

from pydantic import BaseModel

from src.config_base import BaseParserConfig
from src.electives.parser import Elective

config_path = Path(__file__).parent / "config.yaml"


class Target(BaseModel):
    sheet_name: str


class Tag(BaseModel):
    alias: str
    type: str
    name: str


class ElectivesParserConfig(BaseParserConfig):
    targets: list[Target]

    semester_tag: Tag
    spreadsheet_id: str
    electives: list[Elective]


electives_config: ElectivesParserConfig = ElectivesParserConfig.from_yaml(config_path)
