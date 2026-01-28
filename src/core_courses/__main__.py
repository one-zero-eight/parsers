import asyncio
import datetime
import json
import os
from collections.abc import Generator
from itertools import groupby

import pandas as pd

from src.core_courses.cell_to_event import CoreCourseEvent, convert_cell_to_event
from src.core_courses.config import Target
from src.core_courses.config import core_courses_config as config
from src.core_courses.event_to_ical import generate_vevents
from src.core_courses.parser import CoreCourseCell, CoreCoursesParser
from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.utils import fetch_xlsx_spreadsheet, get_base_calendar, get_sheet_gids, sanitize_sheet_name, sluggify


def use(
    processed_column: pd.Series,
    target: Target,
) -> Generator[CoreCourseEvent, None, None]:
    """
    Generate events from processed cells

    :param processed_column: series with processed cells (CoreCourseCell),
        multiindex with (weekday, timeslot) and (course, group) as name
    :param target: target to generate events for (needed for start and end dates)
    :return: generator of events
    """
    # -------- Iterate over processed cells --------
    (course, group) = processed_column.name
    course: str
    group: str

    for (weekday, timeslot), cell in processed_column.items():
        cell: CoreCourseCell | None
        if cell is None:
            continue
        weekday: str
        timeslot: tuple[datetime.time, datetime.time]

        event = convert_cell_to_event(
            cell=cell,
            weekday=weekday,
            timeslot=timeslot,
            course=course,
            group=group,
            target=target,
        )

        if event is None:
            continue

        # Set sheet_name from target
        event.sheet_name = target.sheet_name
        yield event


async def main():
    parser = CoreCoursesParser()
    xlsx_file = await fetch_xlsx_spreadsheet(spreadsheet_id=config.spreadsheet_id)
    original_target_sheet_names = [target.sheet_name for target in config.targets]
    pipeline_result = parser.pipeline(xlsx_file, original_target_sheet_names)
    
    # Get sheet name -> gid mapping
    logger.info("Fetching sheet gids from Google Spreadsheet...")
    sheet_gids = await get_sheet_gids(config.spreadsheet_id)
    logger.debug(f"Found sheet gids: {sheet_gids}")

    # -------- Generate events from processed cells --------
    events: list[CoreCourseEvent] = []
    for target, grouped_dfs_with_cells_list in zip(config.targets, pipeline_result):
        for grouped_dfs_with_cells in grouped_dfs_with_cells_list:
            series_with_generators = grouped_dfs_with_cells.apply(
                use,
                target=target,
            )
            for generator in series_with_generators:
                generator: Generator[CoreCourseEvent, None, None]
                events.extend(generator)

    predefined_event_groups: list[CreateEventGroup] = []

    events.sort(key=lambda x: (x.course, x.group))
    directory = config.save_ics_path
    academic_tag = CreateTag(
        alias="core-courses",
        name="Core courses",
        type="category",
    )
    semester_tag = CreateTag(
        alias=config.semester_tag.alias,
        name=config.semester_tag.name,
        type=config.semester_tag.type,
    )

    logger.info("Writing JSON and iCalendars files...")
    logger.info(f"> Mount point: {config.mount_point}")

    tags = [academic_tag, semester_tag]
    for (course, group), group_events in groupby(events, lambda x: (x.course, x.group)):
        course_slug = sluggify(course)
        course_tag = CreateTag(
            alias=course_slug,
            name=course,
            type="core-courses",
        )
        tags.append(course_tag)

        group_calendar = get_base_calendar()
        group_calendar["x-wr-calname"] = group
        group_calendar["x-wr-link"] = f"https://docs.google.com/spreadsheets/d/{config.spreadsheet_id}"

        group_events = list(group_events)
        cnt = 0
        for group_event in group_events:
            if group_event.subject in config.ignored_subjects:
                logger.debug(f"> Ignoring {group_event.subject}")
                continue
            group_event: CoreCourseEvent
            
            # Get gid for this event's sheet
            gid = None
            if group_event.sheet_name:
                # Try exact match first
                gid = sheet_gids.get(group_event.sheet_name)
                # If not found, try sanitized match
                if gid is None:
                    sanitized_name = sanitize_sheet_name(group_event.sheet_name)
                    for sheet_name, sheet_gid in sheet_gids.items():
                        if sanitize_sheet_name(sheet_name) == sanitized_name:
                            gid = sheet_gid
                            break
                if gid is None:
                    logger.warning(f"Could not find gid for sheet '{group_event.sheet_name}', using first available gid")
                    gid = next(iter(sheet_gids.values())) if sheet_gids else "0"
            else:
                logger.warning("Event has no sheet_name, using first available gid")
                gid = next(iter(sheet_gids.values())) if sheet_gids else "0"
            
            group_vevents = generate_vevents(group_event, config.spreadsheet_id, gid)
            for vevent in group_vevents:
                cnt += 1
                group_calendar.add_component(vevent)
        group_calendar.add("x-wr-total-vevents", str(cnt))

        group_slug = sluggify(group)
        group_alias = f"{semester_tag.alias}-{group_slug}"
        course_path = directory / course_slug
        course_path.mkdir(parents=True, exist_ok=True)
        file_name = f"{group_slug}.ics"
        file_path = course_path / file_name

        logger.info(f"> Writing {file_path}")

        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, "wb") as f:
            content = group_calendar.to_ical()
            # TODO: add validation
            f.write(content)

        predefined_event_groups.append(
            CreateEventGroup(
                alias=group_alias,
                name=group,
                description=f"Core courses schedule for '{group}'",
                path=file_path.relative_to(config.mount_point).as_posix(),
                tags=[
                    academic_tag,
                    semester_tag,
                    course_tag,
                ],
            )
        )

    logger.info(f"Writing JSON file... {len(predefined_event_groups)} event groups.")
    output = Output(event_groups=predefined_event_groups, tags=tags)
    # create a new .json file with information about calendar
    with open(config.save_json_path, "w") as f:
        json.dump(output.model_dump(), f, indent=2, sort_keys=False, ensure_ascii=False)

    # InNoHassle integration
    if config.innohassle_api_url is None or config.parser_auth_key is None:
        logger.info("Skipping InNoHassle integration")
        return

    inh_client = InNoHassleEventsClient(
        api_url=config.innohassle_api_url,
        parser_auth_key=config.parser_auth_key.get_secret_value(),
    )

    result = await update_inh_event_groups(inh_client, config.mount_point, output)
    return result


if __name__ == "__main__":
    asyncio.run(main())
