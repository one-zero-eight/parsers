import asyncio
import datetime
import logging
from typing import Iterable

import aiohttp as aiohttp

from schedule.sports.config import sports_config as config
from schedule.sports.models import (
    ResponseSports,
    ResponseSportSchedule,
)


class SportParser:
    logger = logging.getLogger(__name__ + "." + "Parser")

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_sports(self) -> ResponseSports:
        url = f"{config.api_url}/sports"
        self.logger.info(f"Getting sports from {url}")
        async with self.session.get(url) as response:
            text = await response.text()
            response_schema = ResponseSports.parse_raw(text)
            self.logger.info(f"Got {len(response_schema.sports)} sports")
            return response_schema

    async def get_sport_schedule(self, sport_id: int) -> ResponseSportSchedule:
        finalDate = config.END_OF_SEMESTER.strftime("%Y-%m-%d")
        url = f"{config.api_url}/calendar/{sport_id}/schedule?start={datetime.date.today()}T00%3A00&end={finalDate}T00%3A00"
        self.logger.info(f"Getting sport schedule from {url}")
        async with self.session.get(url) as response:
            text = await response.text()
            response_schema = ResponseSportSchedule.parse_raw(text)
            self.logger.info(f"Got {len(response_schema.__root__)} events")
            return response_schema

    async def batch_get_sport_schedule(
        self, sport_ids: Iterable[int]
    ) -> dict[int, ResponseSportSchedule]:
        tasks = {}
        for sport_id in sport_ids:
            task = asyncio.create_task(self.get_sport_schedule(sport_id))
            tasks[sport_id] = task

        await asyncio.gather(*tasks.values())
        self.logger.info("Got all sport schedules")
        sport_schedules = {sport_id: task.result() for sport_id, task in tasks.items()}
        return sport_schedules
