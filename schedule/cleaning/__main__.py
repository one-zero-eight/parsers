import logging
from typing import Iterable, Any

from pydantic import BaseModel, Field

from schedule.cleaning.parser import CleaningParser, CleaningEvent, LinenChangeEvent
from schedule.cleaning.config import cleaning_config as config
from schedule.models import PredefinedEventGroup, PredefinedTag
from schedule.processors.regex import sluggify
from itertools import groupby
import icalendar

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = CleaningParser(config)
    logger = CleaningParser.logger
    cleaning_events = parser.get_cleaning_events()

    logging.info(f"Cleaning events: {len(cleaning_events)}")

    directory = config.SAVE_ICS_PATH
    json_file = config.SAVE_JSON_PATH
    event_groups = []

    cleaning_events = sorted(cleaning_events, key=lambda x: x.location)

    cleaning_tag = PredefinedTag(
        alias="cleaning",
        name="Cleaning",
        type="category",
    )

    cleaning_cleaning_tag = PredefinedTag(
        alias="room-cleaning",
        name="Room Cleaning",
        type="cleaning",
    )

    for location, events in groupby(cleaning_events, key=lambda x: x.location):
        events: Iterable[CleaningEvent]
        calendar = get_base_calendar()

        calendar["x-wr-calname"] = f"Cleaning: {location}"
        vevents = []

        for event in events:
            vevents.append(event.get_vevent())

        for vevent in vevents:
            calendar.add_component(vevent)

        group_alias = f"cleaning-{sluggify(location)}"
        filename = f"{group_alias}.ics"
        file_path = directory / filename

        event_groups.append(
            PredefinedEventGroup(
                alias=group_alias,
                name=f"Cleaning: {location}",
                description=f"Cleaning schedule for {location}",
                tags=[cleaning_tag, cleaning_cleaning_tag],
                path=file_path.relative_to(config.MOUNT_POINT).as_posix(),
            )
        )
        logger.info(f"Saving {file_path}")
        with open(file_path, "wb") as f:
            f.write(calendar.to_ical())

    linen_change_events = parser.get_linen_change_schedule()

    logging.info(f"Linen change events: {len(linen_change_events)}")

    linen_change_tag = PredefinedTag(
        alias="linen-change",
        name="Linen Change",
        type="cleaning",
    )

    linen_change_events = sorted(linen_change_events, key=lambda x: x.location)

    for location, events in groupby(linen_change_events, key=lambda x: x.location):
        events: Iterable[LinenChangeEvent]

        calendar = get_base_calendar()
        calendar["x-wr-calname"] = f"Linen Change: {location}"

        vevents = []

        for event in events:
            vevents.append(event.get_vevent())

        for vevent in vevents:
            calendar.add_component(vevent)

        group_alias = f"linen-change-{sluggify(location)}"
        filename = f"{group_alias}.ics"
        file_path = directory / filename

        event_groups.append(
            PredefinedEventGroup(
                alias=group_alias,
                name=f"Linen Change: {location}",
                description=f"Linen change schedule for {location}",
                tags=[
                    cleaning_tag,
                    linen_change_tag,
                ],
                path=file_path.relative_to(config.MOUNT_POINT).as_posix(),
            )
        )
        logger.info(f"Saving {file_path}")
        with open(file_path, "wb") as f:
            f.write(calendar.to_ical())

    output = Output(
        event_groups=event_groups,
        tags=[cleaning_tag, linen_change_tag, cleaning_cleaning_tag],
    )

    logger.info(f"Saving calendars information to {json_file}")

    with open(json_file, "w") as f:
        f.write(output.json(indent=2, sort_keys=False))
