import asyncio
import json
from itertools import groupby
from pathlib import Path
from typing import Iterable

from src.cleaning.config import core_courses_config as config
from src.cleaning.parser import CleaningEvent, CleaningParser, LinenChangeEvent
from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.utils import get_base_calendar, sluggify


def main():
    cleaning_tag = CreateTag(alias="cleaning", name="Cleaning", type="category")
    cleaning_cleaning_tag = CreateTag(alias="room-cleaning", name="Room Cleaning", type="cleaning")
    linen_change_tag = CreateTag(alias="linen-change", name="Linen Change", type="cleaning")
    parser = CleaningParser()

    directory = config.SAVE_ICS_PATH
    tags = [cleaning_tag, cleaning_cleaning_tag, linen_change_tag]
    event_groups = []

    # ----- Cleaning schedule -----
    cleaning_events = parser.get_cleaning_events()
    cleaning_events = sorted(cleaning_events, key=lambda x: x.location)
    course_path = Path()
    course_path.mkdir(parents=True, exist_ok=True)
    for location, events in groupby(cleaning_events, key=lambda x: x.location):
        events: Iterable[CleaningEvent]
        calendar = get_base_calendar()
        calendar["x-wr-calname"] = f"Cleaning: {location}"
        calendar["x-wr-link"] = config.cleaning_spreadsheet_url
        cnt = 0
        for event in events:
            cnt += 1
            vevent = event.get_vevent()
            calendar.add_component(vevent)
        calendar.add("x-wr-total-vevents", str(cnt))

        group_alias = f"cleaning-{sluggify(location)}"
        file_path = directory / f"{group_alias}.ics"
        logger.info(f"> Writing {file_path.relative_to(config.MOUNT_POINT)}")

        with open(file_path, "wb") as f:
            content = calendar.to_ical()
            f.write(content)

        event_groups.append(
            CreateEventGroup(
                alias=group_alias,
                name=f"Cleaning: {location}",
                description=f"Cleaning schedule for {location}",
                tags=[cleaning_tag, cleaning_cleaning_tag],
                path=file_path.relative_to(config.MOUNT_POINT).as_posix(),
            )
        )
    # ----- Linen change -----
    linen_change_events = parser.get_linen_change_schedule()
    linen_change_events = sorted(linen_change_events, key=lambda x: x.location)

    for location, events in groupby(linen_change_events, key=lambda x: x.location):
        events: Iterable[LinenChangeEvent]

        calendar = get_base_calendar()
        calendar["x-wr-calname"] = f"Linen Change: {location}"

        for event in events:
            vevent = event.get_vevent()
            calendar.add_component(vevent)

        group_alias = f"linen-change-{sluggify(location)}"
        file_path = directory / f"{group_alias}.ics"
        logger.info(f"> Writing {file_path.relative_to(config.MOUNT_POINT)}")

        with open(file_path, "wb") as f:
            content = calendar.to_ical()
            f.write(content)

        event_groups.append(
            CreateEventGroup(
                alias=group_alias,
                name=f"Linen Change: {location}",
                description=f"Linen change schedule for {location}",
                tags=[cleaning_tag, linen_change_tag],
                path=file_path.relative_to(config.MOUNT_POINT).as_posix(),
            )
        )

    # --- Writing JSON file and InNoHassle integration -
    logger.info(f"Writing JSON file... {len(event_groups)} event groups.")
    output = Output(event_groups=event_groups, tags=tags)
    # create a new .json file with information about calendar
    with open(config.SAVE_JSON_PATH, "w") as f:
        json.dump(output.dict(), f, indent=2, sort_keys=False)

    # InNoHassle integration
    if config.INNOHASSLE_API_URL is None or config.PARSER_AUTH_KEY is None:
        logger.info("Skipping InNoHassle integration")
        return

    inh_client = InNoHassleEventsClient(
        api_url=config.INNOHASSLE_API_URL,
        parser_auth_key=config.PARSER_AUTH_KEY.get_secret_value(),
    )

    result = asyncio.run(update_inh_event_groups(inh_client, config.MOUNT_POINT, output))
    return result


if __name__ == "__main__":
    print(main())
