import asyncio
import datetime

import aiohttp as aiohttp

from schedule.sport.config import sports_config as config
from schedule.sport.models import ResponseSports, ResponseSportSchedule


class SportParser:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_sports(self) -> ResponseSports:
        async with self.session.get(f"{config.api_url}/sports") as response:
            text = await response.text()
            response_schema = ResponseSports.parse_raw(text)
            return response_schema

    async def get_sport_schedule(self, sport_id: int) -> ResponseSportSchedule:
        finalDate = config.END_OF_SEMESTER.strftime("%Y-%m-%d")
        url = f"{config.api_url}/calendar/{sport_id}/schedule?start={datetime.date.today()}T00%3A00&end={finalDate}T00%3A00"
        async with self.session.get(url) as response:
            text = await response.text()
            response_schema = ResponseSportSchedule.parse_raw(text)
            return response_schema


async def main():
    async with aiohttp.ClientSession(
        headers={"Content-Type": "application/json"}
    ) as session:
        parser = SportParser(session)

        get_sports_answer = await parser.get_sports()
        print(
            get_sports_answer.json(
                indent=4,
            )
        )

        tasks = {}
        for sport in get_sports_answer.sports:
            task = asyncio.create_task(parser.get_sport_schedule(sport.id))
            tasks[sport.id] = task

        await asyncio.gather(*tasks.values())

        sport_schedules = {sport_id: task.result() for sport_id, task in tasks.items()}

        for sport_id, sport_schedule in sport_schedules.items():
            print(sport_id)
            print(sport_schedule.json(indent=4))


if __name__ == "__main__":
    asyncio.run(main())
