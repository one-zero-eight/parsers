import asyncio
import json
import os
from itertools import groupby
from pathlib import Path

import aiohttp

from src.config_base import SaveConfig, from_yaml
from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.sports.config import SportsParserConfig
from src.sports.models import SportScheduleEvent
from src.sports.parser import SportParser
from src.utils import get_base_calendar, sluggify


async def main():
    config_path = Path(__file__).parent / "config.yaml"
    parser_config = from_yaml(SportsParserConfig, config_path)
    save_config = from_yaml(SaveConfig, config_path)
    async with aiohttp.ClientSession(headers={"Content-Type": "application/json"}) as session:
        parser = SportParser(session, parser_config)

        get_sports_answer = await parser.get_sports()
        sports = {sport.id: sport for sport in get_sports_answer.sports}
        sport_schedules = await parser.batch_get_sport_schedule(sports.keys())

    sport_events = []

    for sport_id, sport_schedule in sport_schedules.items():
        sport = sports[sport_id]
        _sport_events = [
            SportScheduleEvent(sport=sport, sport_schedule_event=sport_schedule_event)
            for sport_schedule_event in sport_schedule.root
        ]
        sport_events.extend(_sport_events)

    logger.info(f"Processed {len(sport_events)} sport events")

    grouping = lambda x: (x.sport.name, x.sport_schedule_event.title or "")  # noqa: E731
    sport_events.sort(key=grouping)

    event_groups = []

    directory = save_config.save_ics_path
    logger.info(f"Saving calendars to {directory}")
    json_file = save_config.save_json_path
    logger.info(f"Saving json to {json_file}")
    logger.info("Grouping events by sport.name and sport_schedule_event.title")

    sport_tag = CreateTag(alias="sports", type="category", name="Sport")

    for (title, subtitle), events in groupby(sport_events, key=grouping):
        calendar = get_base_calendar()

        calendar_name = f"{title} - {subtitle}" if subtitle else title
        logger.info(f"Saving {calendar_name} calendar")
        calendar["x-wr-calname"] = calendar_name
        calendar["x-wr-link"] = "https://sport.innopolis.university"
        for event in events:
            event: SportScheduleEvent
            vevent = event.get_vevent(parser_config.start_of_semester, parser_config.end_of_semester)
            calendar.add_component(vevent)

        group_alias = sluggify(calendar_name)

        filename = f"{group_alias}.ics"
        file_path = directory / filename
        event_groups.append(
            CreateEventGroup(
                alias=group_alias,
                name=calendar_name,
                description=f"Sport schedule for '{calendar_name}'",
                path=file_path.relative_to(save_config.mount_point).as_posix(),
                tags=[sport_tag],
            )
        )
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, "wb") as file:
            file.write(calendar.to_ical())

    output = Output(event_groups=event_groups, tags=[sport_tag])

    logger.debug(f"Saving calendars information to {json_file}")
    os.makedirs(json_file.parent, exist_ok=True)
    with open(json_file, "w") as f:
        json.dump(output.model_dump(), f, indent=2, sort_keys=False, ensure_ascii=False)

    # InNoHassle integration
    if save_config.innohassle_api_url is None or save_config.parser_auth_key is None:
        logger.info("Skipping InNoHassle integration")
        return

    inh_client = InNoHassleEventsClient(
        api_url=save_config.innohassle_api_url,
        parser_auth_key=save_config.parser_auth_key.get_secret_value(),
    )

    return await update_inh_event_groups(inh_client, save_config.mount_point, output)


if __name__ == "__main__":
    asyncio.run(main())
