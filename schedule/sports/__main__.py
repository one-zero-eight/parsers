import asyncio
import json
import logging
from itertools import groupby
from typing import Any

import aiohttp as aiohttp
import icalendar
from pydantic import BaseModel, Field

from schedule.models import PredefinedEventGroup, PredefinedTag
from schedule.processors.regex import sluggify
from schedule.sports.config import sports_config as config
from schedule.sports.models import SportScheduleEvent
from schedule.sports.parser import SportParser
from schedule.utils import get_base_calendar


class Output(BaseModel):
    event_groups: list[PredefinedEventGroup]
    tags: list[PredefinedTag]
    meta: dict[str, Any] = Field(default_factory=dict)

    def __init__(
        self,
        event_groups: list[PredefinedEventGroup],
        tags: list[PredefinedTag],
    ):
        # only unique (alias, type) tags
        visited = set()

        visited_tags = []

        for tag in tags:
            if (tag.alias, tag.type) not in visited:
                visited.add((tag.alias, tag.type))
                visited_tags.append(tag)

        # sort tags
        visited_tags = sorted(visited_tags, key=lambda x: (x.type, x.alias))

        super().__init__(event_groups=event_groups, tags=visited_tags)

        self.meta = {
            "event_groups_count": len(self.event_groups),
            "tags_count": len(self.tags),
        }


async def main():
    async with aiohttp.ClientSession(
        headers={"Content-Type": "application/json"}
    ) as session:
        logging.basicConfig(level=logging.INFO)
        parser = SportParser(session)
        logger = SportParser.logger

        get_sports_answer = await parser.get_sports()
        sports = {sport.id: sport for sport in get_sports_answer.sports}
        sport_schedules = await parser.batch_get_sport_schedule(sports.keys())

        sport_events = []

        for sport_id, sport_schedule in sport_schedules.items():
            sport = sports[sport_id]
            _sport_events = [
                SportScheduleEvent(
                    sport=sport, sport_schedule_event=sport_schedule_event
                )
                for sport_schedule_event in sport_schedule.__root__
            ]
            sport_events.extend(_sport_events)
        logger.info(f"Processed {len(sport_events)} sport events")

        grouping = lambda x: (x.sport.name, x.sport_schedule_event.title or "")
        sport_events.sort(key=grouping)

        event_groups = []

        directory = config.SAVE_ICS_PATH
        logger.info(f"Saving calendars to {directory}")
        json_file = config.SAVE_JSON_PATH
        logger.info(f"Saving json to {json_file}")
        logger.info(f"Grouping events by sport.name and sport_schedule_event.title")

        sport_tag = PredefinedTag(alias="sports", type="category", name="Sport")

        sport_tag_reference = sport_tag.reference

        for (title, subtitle), events in groupby(sport_events, key=grouping):
            calendar = get_base_calendar()

            calendar_name = f"{title} - {subtitle}" if subtitle else title
            logger.info(f"Saving {calendar_name} calendar")
            calendar["x-wr-calname"] = calendar_name
            for event in events:
                event: SportScheduleEvent
                vevent = event.get_vevent(
                    config.START_OF_SEMESTER, config.END_OF_SEMESTER
                )
                calendar.add_component(vevent)

            group_alias = sluggify(calendar_name)

            filename = f"{group_alias}.ics"
            file_path = directory / filename
            event_groups.append(
                PredefinedEventGroup(
                    alias=group_alias,
                    name=calendar_name,
                    description=f"Sport schedule for '{calendar_name}'",
                    path=file_path.relative_to(config.MOUNT_POINT).as_posix(),
                    tags=[sport_tag_reference],
                )
            )

            with open(file_path, "wb") as file:
                file.write(calendar.to_ical())

        output = Output(event_groups=event_groups, tags=[sport_tag])

        logger.info(f"Saving calendars information to {json_file}")
        with open(json_file, "w") as f:
            json.dump(output.dict(), f, indent=2, sort_keys=False)

        logger.info("Done")


if __name__ == "__main__":
    asyncio.run(main())
