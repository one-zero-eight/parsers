import datetime
import itertools
import json
import logging
import re

import icalendar

from schedule.core_courses.config import core_courses_config as config
from schedule.core_courses.models import Subject, ScheduleEvent
from schedule.core_courses.parser import CoreCoursesParser, process_target_schedule
from schedule.core_courses.temp import history_events
from schedule.utils import get_base_calendar

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    ignoring_subjects = [
        Subject.from_str(subject_name) for subject_name in config.IGNORING_SUBJECTS
    ]
    for subject in ignoring_subjects:
        subject.is_ignored = True

    parser = CoreCoursesParser()
    logger = CoreCoursesParser.logger
    all_events = []
    for i in range(len(config.TARGET_RANGES)):
        logger.info(f"Processing target {i}")
        all_events.extend(process_target_schedule(parser, i))

    calendars = {
        "filters": [{"title": "Course", "alias": "course"}],
        "title": "Core Courses",
        "calendars": [],
    }

    directory = config.SAVE_ICS_PATH
    json_file = config.SAVE_JSON_PATH

    # replace spaces and dashes with single dash
    replace_spaces_pattern = re.compile(r"[\s-]+")

    all_events = sorted(all_events, key=lambda x: (x.course, x.group))
    now_str = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    logger.info("Writing JSON and iCalendars files...")
    for course_name, course_events in itertools.groupby(all_events, lambda x: x.course):
        logger.info(f" > Writing course {course_name}")
        course_path = directory / replace_spaces_pattern.sub("-", course_name)
        course_path.mkdir(parents=True, exist_ok=True)
        for group_name, group_events in itertools.groupby(
            course_events, lambda x: x.group
        ):
            logger.info(f"  > {group_name}...")
            calendar = get_base_calendar()
            calendar["x-wr-calname"] = group_name
            vevents = []

            for group_event in group_events:
                if group_event.subject.is_ignored:
                    logger.info(f"   > Ignoring {group_event.subject.name}")
                    continue
                group_event: ScheduleEvent
                group_vevents = group_event.get_vevents()
                vevents.extend(group_vevents)
                for vevent in group_vevents:
                    calendar.add_component(vevent)

            if course_name == "BS - Year 1":
                for event in history_events:
                    history_vevents = event.get_vevents()
                    vevents.extend(history_vevents)
                    for vevent in history_vevents:
                        calendar.add_component(vevent)

            file_name = f"{group_name}.ics"
            file_path = course_path / file_name
            calendar_name = group_name
            calendars["calendars"].append(
                {
                    "name": calendar_name,
                    "path": file_path.relative_to(json_file.parent).as_posix(),
                    "type": "core course",
                    "satellite": {"course": course_name},
                }
            )

            with open(file_path, "wb") as f:
                f.write(calendar.to_ical())

    # create a new .json file with information about calendars
    with open(json_file, "w") as f:
        json.dump(calendars, f, indent=4, sort_keys=True)
