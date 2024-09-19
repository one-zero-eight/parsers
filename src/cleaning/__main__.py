import asyncio
import datetime
from os import environ
from pathlib import Path

from pydantic import BaseModel
from dotenv import load_dotenv

from src.innohassle import InNoHassleEventsClient
from src.logging_ import logger
from src.utils import get_project_root

PROJECT_ROOT = get_project_root()
CONFIG_PATH = Path(__file__).parent / "config.json"


class CleaningEntry(BaseModel):
    name: str = "Cleaning"
    location: str
    dates: list[datetime.date]


class LinenChangeEntry(BaseModel):
    name: str = "Linen change"
    location: str
    rrule: dict[str, str]


class CleaningParserConfig(BaseModel):
    start_date: datetime.date
    cleaning_entries: list[CleaningEntry]
    linen_change_entries: list[LinenChangeEntry]


async def main():
    config: CleaningParserConfig = CleaningParserConfig.parse_file(CONFIG_PATH)
    load_dotenv()
    api_url = environ.get("INNOHASSLE_API_URL")
    parser_auth_key = environ.get("PARSER_AUTH_KEY")

    # InNoHassle integration
    if api_url is None or parser_auth_key is None:
        logger.info("Skipping InNoHassle integration")
        return

    inh_client = InNoHassleEventsClient(api_url=api_url, parser_auth_key=parser_auth_key)
    await inh_client.parse_cleaning(config)
    logger.info("Cleaning schedule is updated")


if __name__ == "__main__":
    asyncio.run(main())
