import asyncio
from collections.abc import Iterable

import aiohttp

from src.logging_ import logger
from src.sports.config import sports_config as config
from src.sports.models import ResponseSports, ResponseSportSchedule


class SportParser:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_sports(self) -> ResponseSports:
        url = f"{config.api_url}/sports"
        logger.debug(f"Getting sports from {url}")
        async with self.session.get(url) as response:
            text = await response.text()
            response_schema = ResponseSports.parse_raw(text)
            logger.debug(f"Got {len(response_schema.sports)} sports")
            return response_schema

    async def get_sport_schedule(self, sport_id: int) -> ResponseSportSchedule:
        start = config.start_of_semester.strftime("%Y-%m-%d")
        final = config.end_of_semester.strftime("%Y-%m-%d")
        url = f"{config.api_url}/calendar/{sport_id}/schedule?start={start}T00%3A00&end={final}T00%3A00"
        logger.debug(f"Getting sport schedule from {url}")
        async with self.session.get(url) as response:
            text = await response.text()
            response_schema = ResponseSportSchedule.model_validate_json(text)
            logger.debug(f"Got {len(response_schema.root)} events")
            return response_schema

    async def batch_get_sport_schedule(self, sport_ids: Iterable[int]) -> dict[int, ResponseSportSchedule]:
        tasks = {}
        for sport_id in sport_ids:
            task = asyncio.create_task(self.get_sport_schedule(sport_id))
            tasks[sport_id] = task

        await asyncio.gather(*tasks.values())
        logger.debug("Got all sport schedules")
        sport_schedules = {sport_id: task.result() for sport_id, task in tasks.items()}
        return sport_schedules
