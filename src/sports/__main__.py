import asyncio
import json
from itertools import groupby

import aiohttp as aiohttp

from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.processors.regex import sluggify
from src.sports.config import sports_config as config
from src.sports.models import SportScheduleEvent
from src.sports.parser import SportParser
from src.utils import get_base_calendar


async def main():
    async with aiohttp.ClientSession(headers={"Content-Type": "application/json"}) as session:
        parser = SportParser(session)

        get_sports_answer = await parser.get_sports()
        sports = {sport.id: sport for sport in get_sports_answer.sports}
        sport_schedules = await parser.batch_get_sport_schedule(sports.keys())

    sport_events = []

    for sport_id, sport_schedule in sport_schedules.items():
        sport = sports[sport_id]
        _sport_events = [
            SportScheduleEvent(sport=sport, sport_schedule_event=sport_schedule_event)
            for sport_schedule_event in sport_schedule.__root__
        ]
        sport_events.extend(_sport_events)

    logger.info(f"Processed {len(sport_events)} sport events")

    grouping = lambda x: (x.sport.name, x.sport_schedule_event.title or "")  # noqa: E731
    sport_events.sort(key=grouping)

    event_groups = []

    directory = config.SAVE_ICS_PATH
    logger.info(f"Saving calendars to {directory}")
    json_file = config.SAVE_JSON_PATH
    logger.info(f"Saving json to {json_file}")
    logger.info("Grouping events by sport.name and sport_schedule_event.title")

    sport_tag = CreateTag(alias="sports", type="category", name="Sport")

    for (title, subtitle), events in groupby(sport_events, key=grouping):
        calendar = get_base_calendar()

        calendar_name = f"{title} - {subtitle}" if subtitle else title
        logger.info(f"Saving {calendar_name} calendar")
        calendar["x-wr-calname"] = calendar_name
        for event in events:
            event: SportScheduleEvent
            vevent = event.get_vevent(config.START_OF_SEMESTER, config.END_OF_SEMESTER)
            calendar.add_component(vevent)

        group_alias = sluggify(calendar_name)

        filename = f"{group_alias}.ics"
        file_path = directory / filename
        event_groups.append(
            CreateEventGroup(
                alias=group_alias,
                name=calendar_name,
                description=f"Sport schedule for '{calendar_name}'",
                path=file_path.relative_to(config.MOUNT_POINT).as_posix(),
                tags=[sport_tag],
            )
        )

        with open(file_path, "wb") as file:
            file.write(calendar.to_ical())

    output = Output(event_groups=event_groups, tags=[sport_tag])

    logger.debug(f"Saving calendars information to {json_file}")
    with open(json_file, "w") as f:
        json.dump(output.dict(), f, indent=2, sort_keys=False)

    # InNoHassle integration
    if config.INNOHASSLE_API_URL is None or config.PARSER_AUTH_KEY is None:
        logger.info("Skipping InNoHassle integration")
        return

    inh_client = InNoHassleEventsClient(
        api_url=config.INNOHASSLE_API_URL,
        parser_auth_key=config.PARSER_AUTH_KEY.get_secret_value(),
    )

    await update_inh_event_groups(inh_client, config.MOUNT_POINT, output)


if __name__ == "__main__":
    asyncio.run(main())
