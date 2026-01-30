import asyncio
import json
import os
from collections.abc import Iterable
from itertools import groupby
from pathlib import Path

from src.cleaning.config import CleaningParserConfig
from src.cleaning.parser import CleaningEvent, CleaningParser, LinenChangeEvent
from src.config_base import SaveConfig, from_yaml
from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.utils import get_base_calendar, sluggify


def main():
    config_path = Path(__file__).parent / "config.yaml"
    parser_config = from_yaml(CleaningParserConfig, config_path)
    save_config = from_yaml(SaveConfig, config_path)
    parser = CleaningParser(parser_config)

    cleaning_tag = CreateTag(alias="cleaning", name="Cleaning", type="category")
    cleaning_cleaning_tag = CreateTag(alias="room-cleaning", name="Room Cleaning", type="cleaning")
    linen_change_tag = CreateTag(alias="linen-change", name="Linen Change", type="cleaning")

    directory = save_config.save_ics_path
    tags = [cleaning_tag, cleaning_cleaning_tag, linen_change_tag]
    event_groups = []

    # ----- Cleaning schedule -----
    cleaning_events = parser.get_cleaning_events()
    cleaning_events = sorted(cleaning_events, key=lambda x: x.location)
    course_path = Path()
    course_path.mkdir(parents=True, exist_ok=True)
    for location, cleaning_events_group in groupby(cleaning_events, key=lambda x: x.location):
        cleaning_events_group: Iterable[CleaningEvent]
        calendar = get_base_calendar()
        calendar["x-wr-calname"] = f"Cleaning: {location}"
        calendar["x-wr-link"] = parser_config.cleaning_spreadsheet_url
        cnt = 0
        for cleaning_event in cleaning_events_group:
            cnt += 1
            vevent = cleaning_event.get_vevent()
            calendar.add_component(vevent)
        calendar.add("x-wr-total-vevents", str(cnt))

        group_alias = f"cleaning-{sluggify(location)}"
        file_path = directory / f"{group_alias}.ics"
        logger.info(f"> Writing {file_path}")

        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, "wb") as f:
            content = calendar.to_ical()
            f.write(content)

        event_groups.append(
            CreateEventGroup(
                alias=group_alias,
                name=f"Cleaning: {location}",
                description=f"Cleaning schedule for {location}",
                tags=[cleaning_tag, cleaning_cleaning_tag],
                path=file_path.relative_to(save_config.mount_point).as_posix(),
            )
        )
    # ----- Linen change -----
    linen_change_events = parser.get_linen_change_schedule()
    linen_change_events = sorted(linen_change_events, key=lambda x: x.location)

    for location, linen_change_events_group in groupby(linen_change_events, key=lambda x: x.location):
        linen_change_events_group: Iterable[LinenChangeEvent]

        calendar = get_base_calendar()
        calendar["x-wr-calname"] = f"Linen Change: {location}"

        for linen_change_event in linen_change_events_group:
            vevent = linen_change_event.get_vevent()
            calendar.add_component(vevent)

        group_alias = f"linen-change-{sluggify(location)}"
        file_path = directory / f"{group_alias}.ics"
        logger.info(f"> Writing {file_path}")

        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, "wb") as f:
            content = calendar.to_ical()
            f.write(content)

        event_groups.append(
            CreateEventGroup(
                alias=group_alias,
                name=f"Linen Change: {location}",
                description=f"Linen change schedule for {location}",
                tags=[cleaning_tag, linen_change_tag],
                path=file_path.relative_to(save_config.mount_point).as_posix(),
            )
        )

    # --- Writing JSON file and InNoHassle integration -
    logger.info(f"Writing JSON file... {len(event_groups)} event groups.")
    output = Output(event_groups=event_groups, tags=tags)
    # create a new .json file with information about calendar
    with open(save_config.save_json_path, "w") as f:
        json.dump(output.model_dump(), f, indent=2, sort_keys=False, ensure_ascii=False)

    # InNoHassle integration
    if save_config.innohassle_api_url is None or save_config.parser_auth_key is None:
        logger.info("Skipping InNoHassle integration")
        return

    inh_client = InNoHassleEventsClient(
        api_url=save_config.innohassle_api_url,
        parser_auth_key=save_config.parser_auth_key.get_secret_value(),
    )

    result = asyncio.run(update_inh_event_groups(inh_client, save_config.mount_point, output))
    return result


if __name__ == "__main__":
    print(main())
