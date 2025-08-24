import asyncio
import io
import json
from hashlib import sha1
from itertools import chain, groupby

import pandas as pd

from src.core_courses.config import core_courses_config as config
from src.core_courses.models import CoreCourseCell, CoreCourseEvent
from src.core_courses.parser import CoreCoursesParser
from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.utils import get_base_calendar, sluggify


# noinspection InsecureHash
def hashsum_dfs(dfs: dict[str, pd.DataFrame]) -> str:
    to_hash = (sha1(pd.util.hash_pandas_object(dfs[target.sheet_name]).values).hexdigest() for target in config.targets)
    hashsum = sha1("\n".join(to_hash).encode("utf-8")).hexdigest()
    return hashsum


def get_dataframes_pipeline(parser: CoreCoursesParser, xlsx: io.BytesIO) -> dict[str, pd.DataFrame]:
    dfs, _merged_ranges = parser.get_clear_dataframes_from_xlsx(
        xlsx_file=xlsx, target_sheet_names=[target.sheet_name for target in config.targets]
    )
    hashsum = hashsum_dfs(dfs)

    logger.info(f"Hashsum: {hashsum}")
    xlsx_path = config.temp_dir / f"{hashsum}.xlsx"
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    if xlsx_path.exists():
        logger.info("Hashsum match!")

    with open(xlsx_path, "wb") as f:
        logger.info(f"Saving cached file {hashsum}.xlsx")
        xlsx.seek(0)
        content = xlsx.read()
        f.write(content)

    return dfs


def main():
    parser = CoreCoursesParser()

    xlsx = parser.get_xlsx_file(spreadsheet_id=config.spreadsheet_id)

    dfs = get_dataframes_pipeline(parser, xlsx)

    events = []

    for target in config.targets:
        logger.info(f"Processing '{target.sheet_name}'...")
        # find dataframe from dfs
        sheet_df = dfs[target.sheet_name]

        time_columns_index = parser.get_time_columns(sheet_df)
        by_courses = parser.split_df_by_courses(sheet_df, time_columns_index)
        for course_df in by_courses:
            # -------- Set course and group as header; weekday and timeslot as index --------
            parser.set_course_and_group_as_header(course_df)
            parser.set_weekday_and_time_as_index(course_df)
            # -------- Process cells and generate events --------
            event_generators = (
                course_df
                # -------- Group by weekday and time --------
                .groupby(level=[0, 1], sort=False)
                .agg(list)
                # -------- Apply CoreCourseCell to each cell --------
                .map(lambda x: None if all(pd.isna(y) for y in x) else CoreCourseCell(value=x))
                # -------- Generate events from processed cells --------
                .apply(parser.generate_events_from_processed_column, target=target)
            )
            # -------- Append generated events to events list --------
            events.extend(chain.from_iterable(event_generators))

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
    courses = set(event.course for event in events)
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
            group_vevents = group_event.generate_vevents()
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

        logger.info(f"> Writing {file_path.relative_to(config.mount_point)}")

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

    result = asyncio.run(update_inh_event_groups(inh_client, config.mount_point, output))
    return result


if __name__ == "__main__":
    main()
