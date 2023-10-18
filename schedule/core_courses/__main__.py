import json
import logging
import os
from hashlib import sha1
from itertools import chain, groupby
from typing import Any

import icalendar
import pandas as pd
from openpyxl.utils import column_index_from_string
from pydantic import BaseModel, Field

from schedule.core_courses.config import core_courses_config as config
from schedule.core_courses.models import CoreCourseCell, CoreCourseEvent
from schedule.core_courses.parser import CoreCoursesParser
from schedule.models import PredefinedEventGroup, PredefinedTag
from schedule.processors.regex import sluggify
from schedule.utils import get_base_calendar


# noinspection InsecureHash
def hashsum_dfs(dfs: dict[str, pd.DataFrame]) -> str:
    to_hash = (
        sha1(pd.util.hash_pandas_object(dfs[target.sheet_name]).values).hexdigest()
        for target in config.TARGETS
    )
    hashsum = sha1("\n".join(to_hash).encode("utf-8")).hexdigest()
    return hashsum


def get_dataframes_pipeline() -> dict[str, pd.DataFrame]:
    dfs = parser.get_clear_dataframes_from_xlsx(xlsx_file=xlsx, targets=config.TARGETS)
    hashsum = hashsum_dfs(dfs)

    parser.logger.info(f"Hashsum: {hashsum}")
    xlsx_path = config.TEMP_DIR / f"{hashsum}.xlsx"

    if xlsx_path.exists():
        parser.logger.info(f"Hashsum match!")

    with open(xlsx_path, "wb") as f:
        parser.logger.info(f"Saving cached file {hashsum}.xlsx")
        xlsx.seek(0)
        content = xlsx.read()
        f.write(content)

    return dfs


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

    parser = CoreCoursesParser()

    xlsx = parser.get_xlsx_file(spreadsheet_id=config.SPREADSHEET_ID)

    dfs = get_dataframes_pipeline()

    events = []

    for target in config.TARGETS:
        parser.logger.info(f"Processing '{target.sheet_name}'... Range: {target.range}")
        # find dataframe from dfs
        sheet_df = dfs[target.sheet_name]

        time_columns_index = [
            column_index_from_string(col) - 1 for col in target.time_columns
        ]
        by_courses = parser.split_df_by_courses(sheet_df, time_columns_index)
        for course_df in by_courses:
            # -------- Set course and group as header; weekday and timeslot as index --------
            parser.set_course_and_group_as_header(course_df)
            parser.set_weekday_and_time_as_index(course_df)
            # -------- Process cells and generate events --------
            event_generators = (
                course_df
                # -------- Group by weekday and time --------
                .groupby(level=[0, 1], sort=False).agg(list)
                # -------- Apply CoreCourseCell to each cell --------
                .applymap(
                    lambda x: None
                    if all(pd.isna(y) for y in x)
                    else CoreCourseCell(value=x)
                )
                # -------- Generate events from processed cells --------
                .apply(parser.generate_events_from_processed_column, target=target)
            )
            # -------- Append generated events to events list --------
            events.extend(chain.from_iterable(event_generators))

    predefined_event_groups: list[PredefinedEventGroup] = []

    events.sort(key=lambda x: (x.course, x.group))
    directory = config.SAVE_ICS_PATH
    academic_tag = PredefinedTag(
        alias="core-courses",
        name="Core courses",
        type="category",
    )
    semester_tag = PredefinedTag(
        alias=config.SEMESTER_TAG.alias,
        name=config.SEMESTER_TAG.name,
        type=config.SEMESTER_TAG.type,
    )
    academic_tag_reference = academic_tag.reference
    semester_tag_reference = semester_tag.reference

    parser.logger.info("Writing JSON and iCalendars files...")
    parser.logger.info(f"> Mount point: {config.MOUNT_POINT}")

    tags = [academic_tag, semester_tag]
    courses = set(event.course for event in events)
    for (course, group), group_events in groupby(events, lambda x: (x.course, x.group)):
        course_slug = sluggify(course)
        course_tag = PredefinedTag(
            alias=course_slug,
            name=course,
            type="core-courses",
        )
        course_tag_reference = course_tag.reference
        tags.append(course_tag)

        group_calendar = get_base_calendar()

        group_calendar["x-wr-calname"] = group
        group_events = list(group_events)
        cnt = 0
        for group_event in group_events:
            if group_event.subject in config.IGNORED_SUBJECTS:
                parser.logger.info(f"> Ignoring {group_event.subject}")
                continue
            group_event: CoreCourseEvent
            group_vevents = group_event.generate_vevents()
            for vevent in group_vevents:
                cnt += 1
                group_calendar.add_component(vevent)
        group_calendar.add("x-wr-total-vevents", str(cnt))

        group_slug = sluggify(group)
        group_alias = f"{semester_tag_reference.alias}-{group_slug}"
        course_path = directory / course_slug
        course_path.mkdir(parents=True, exist_ok=True)
        file_name = f"{group_slug}.ics"
        file_path = course_path / file_name

        parser.logger.info(f"> Writing {file_path.relative_to(config.MOUNT_POINT)}")

        with open(file_path, "wb") as f:
            content = group_calendar.to_ical()
            # TODO: add validation
            f.write(content)

        predefined_event_groups.append(
            PredefinedEventGroup(
                alias=group_alias,
                name=group,
                description=f"Core courses schedule for '{group}'",
                path=file_path.relative_to(config.MOUNT_POINT).as_posix(),
                tags=[
                    academic_tag_reference,
                    semester_tag_reference,
                    course_tag_reference,
                ],
            )
        )

    parser.logger.info(
        f"Writing JSON file... {len(predefined_event_groups)} event groups."
    )
    output = Output(event_groups=predefined_event_groups, tags=tags)
    # create a new .json file with information about calendar
    with open(config.SAVE_JSON_PATH, "w") as f:
        json.dump(output.dict(), f, indent=2, sort_keys=False)
