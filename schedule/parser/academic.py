import datetime
import itertools
import json
import logging
import re
from collections import defaultdict
from itertools import pairwise
from pathlib import Path
from typing import Optional, Iterable
from uuid import uuid4

import googleapiclient.discovery
import icalendar
import numpy as np
import pandas as pd
from google.oauth2.credentials import Credentials
from pydantic import BaseModel, Field

from config import academic_config as config, PARSER_PATH
from utils import *
import hashlib

CURRENT_YEAR = datetime.datetime.now().year


class Subject(BaseModel):
    name: str
    is_ignored: bool = False

    @classmethod
    def from_str(cls, s: str):
        # "Software Project  (lec)                  " -> "Software Project"
        # "Software Project  (lab)                  " -> "Software Project"
        # "Theoretical sports(lec)- Physiology of sports									" -> "Theoretical sports"
        s = re.sub(r"\s+\(.*\)\s*$", "", s)
        s = re.sub(r"\s+-.*$", "", s)
        s = re.sub(r"\s+$", "", s)

        if s not in cls.__instances__:
            cls.__instances__[s] = cls(name=s)
        return cls.__instances__[s]

    @classmethod
    def get(cls, s: str) -> Optional['Subject']:
        return cls.__instances__.get(s)

    @classmethod
    def get_all(cls):
        return list(cls.__instances__.values())

    __instances__ = {}


class Flags(BaseModel):
    only_on_specific_date: bool | datetime.date = False


class ScheduleEvent(BaseModel):
    subject: Optional[Subject]
    start_time: Optional[datetime.time]
    end_time: Optional[datetime.time]
    day: Optional[datetime.date]
    dtstamp: Optional[datetime.datetime]
    location: Optional[str]
    instructor: Optional[str]
    event_type: Optional[str]
    recurrence: Optional[list[dict]]
    flags: Flags = Field(default_factory=Flags)
    group: Optional[str]
    course: Optional[str]

    @property
    def summary(self):
        r = f"{self.subject.name}"
        if self.event_type:
            r += f" ({self.event_type})"
        return r

    @property
    def description(self):
        r = {
            "Location": self.location,
            "Instructor": self.instructor,
            "Type": self.event_type,
            "Group": self.group,
            "Subject": self.subject.name,
            "Time": f"{self.start_time.strftime('%H:%M')} - {self.end_time.strftime('%H:%M')}"

        }

        r = {k: v for k, v in r.items() if v}
        return "\n".join([f"{k}: {v}" for k, v in r.items()])

    @property
    def dtstart(self):
        return datetime.datetime.combine(self.day, self.start_time)

    @property
    def dtend(self):
        return datetime.datetime.combine(self.day, self.end_time)

    def __hash__(self):
        return hash((self.subject.name, self.event_type, self.start_time, self.end_time, self.group))

    def get_uid(self) -> str:
        return '%x@innopolis.ru' % abs(hash(self))

    def __eq__(self, other: 'ScheduleEvent'):
        return (self.subject == other.subject and
                self.event_type == other.event_type and
                self.start_time == other.start_time and
                self.end_time == other.end_time and
                self.group == other.group)

    def from_cell(self, lines: list[str]):
        # lines = [pretty for line in lines if (pretty := remove_trailing_spaces(line))]
        iterator = filter(None, lines)
        _title = next(iterator, None)
        subject = Subject.from_str(_title)
        instructor = next(iterator, None)
        location = next(iterator, None)

        only_on = False

        if location:
            # "108 (ONLY ON 14/06)" -> "108", only_on=datetime(6, 14)
            if match := re.search(r"\(ONLY ON (\d+)/(\d+)\)", location):
                location = location[:match.start()].strip()
                day_ = int(match.group(1))
                month_ = int(match.group(2))
                only_on = datetime.datetime(CURRENT_YEAR, day=day_, month=month_).date()
        event_type = None

        if match := re.search(r"\((.+)\)", _title):
            # "Software Project (lec)" -> "lec"
            # "Software Project (lab )" -> "lab"
            event_type = match.group(1)
            # remove spaces
            event_type = re.sub(r"\s+", "", event_type)

        if subject:
            self.subject = subject
        if instructor:
            self.instructor = instructor
        if location:
            self.location = location
        if event_type:
            self.event_type = event_type
        if only_on:
            self.flags.only_on_specific_date = only_on


class AcademicParser:
    spreadsheets: googleapiclient.discovery.Resource
    credentials: Credentials
    logger = logging.getLogger(__name__ + "." + "Parser")

    def __init__(self):
        self.credentials = get_credentials(
            Path(config.CREDENTIALS_PATH),
            token_path=Path(PARSER_PATH / "token.json"),
            scopes=config.API_SCOPES
        )
        self.spreadsheets = connect_spreadsheets(self.credentials)

    @staticmethod
    def merge_cells(df: pd.DataFrame, target_sheet: dict):

        if "merges" not in target_sheet:
            return

        max_x, max_y = df.shape

        for merge in target_sheet["merges"]:
            x0 = merge["startRowIndex"]
            y0 = merge["startColumnIndex"]
            x1 = merge["endRowIndex"]
            y1 = merge["endColumnIndex"]

            if x0 < max_x and y0 < max_y:
                df.iloc[x0: x1, y0: y1] = df.iloc[x0][y0]

    def get_clear_df(
            self,
            spreadsheet_id: str,
            target_range: str,
            target_title: str
    ) -> pd.DataFrame:
        """Get data from Google Sheets and return it as a DataFrame with merged cells and empty cells in the course
        row filled by left value. Also remove trailing spaces and translate russian letters to english ones."""

        self.logger.debug("Getting dataframe from Google Sheets...")
        self.logger.info(f"Retrieving data: {spreadsheet_id}/{target_title}-{target_range}")

        values = self.spreadsheets.values().get(
            spreadsheetId=spreadsheet_id,
            range=target_range
        ).execute()["values"]

        df = pd.DataFrame(data=values)
        # remove trailing spaces and translate
        df.replace(r'^\s*$', '', regex=True, inplace=True)
        df = df.applymap(lambda x: beautify_string(x) if isinstance(x, str) else x)

        max_x, max_y = df.shape

        self.logger.info(f"Data retrieved: {max_x}x{max_y}")
        spreadsheet = self.spreadsheets.get(
            spreadsheetId=config.SPREADSHEET_ID,
            ranges=[target_range],
            includeGridData=False  # values already fetched
        ).execute()

        self.logger.info(
            f"Spreadsheet {spreadsheet['properties']['title']} retrieved:\n" +
            f"Sheets({len(spreadsheet['sheets'])}): " + ", ".join(
                f"{sheet['properties']['title']}"
                for sheet in spreadsheet['sheets'])
        )

        # get target sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == target_title:
                target_sheet = sheet
                break

        if target_sheet is None:
            raise ValueError(f"Target sheet {target_title} not found")

        self.logger.info(
            f"Target sheet: {target_sheet['properties']['title']} " +
            f"Sheet index: ({target_sheet['properties']['index']}) " +
            f"Sheet merges: ({len(target_sheet['merges'])})"
        )

        self.logger.info("Merging cells")

        self.merge_cells(df, target_sheet)

        df.fillna('', inplace=True)

        self.logger.info("Cells merged")
        self.logger.info("Filling empty cells")
        for y in range(1, max_y):
            course_name = df.iloc[0, y]
            if course_name == '':
                df.iloc[0, y] = df.iloc[0, y - 1]
                self.logger.info(f"> Filled empty cell in courses line: {y}")
        self.logger.info("Empty cells filled")
        self.logger.info("Dataframe ready")
        return df

    @staticmethod
    def refactor_course_df(
            course_df: pd.DataFrame,
            group_names: list[str]
    ) -> pd.DataFrame:
        """Refactor course DataFrame to get a DataFrame with one cell corresponding to pair (timeslot, group),
        to one event. """
        course_df.columns = ["time", *group_names]
        course_df.set_index("time", inplace=True)

        course_df = course_df.groupby("time").agg(lambda intersecting: list(filter(None, intersecting)))

        # course_df.fillna('', inplace=True)

        return course_df

    def parse_df(self, df: pd.DataFrame) -> dict[str, dict[str, list[ScheduleEvent]]]:
        """Parse DataFrame into a dictionary with separation by days and then by course. """

        self.logger.debug("Parsing dataframe to separation by days|groups...")
        self.logger.info("Get 'week' indexes...")
        week_column = df.iloc[:, 0]
        week_mask = week_column.isin(config.DAYS).values
        week_indexes = np.argwhere(week_mask).flatten().tolist()
        self.logger.info(f"> Found {len(week_indexes)} indexes:")

        self.logger.info("Separating by days...")
        max_x, max_y = df.shape
        separation_by_days = defaultdict(dict)
        courses_line = df.iloc[0, 1:]
        groups_line = df.iloc[1, 1:]

        for start_x, end_x in pairwise(week_indexes + [max_x]):
            day_name = week_column[start_x]
            self.logger.info(f" > Separating day {day_name}")
            week_df = df.iloc[start_x:end_x]
            day_row = week_df.iloc[0]
            day_mask = day_row.isin(config.DAYS).values
            day_indexes = np.argwhere(day_mask).flatten().tolist()

            for start_y, end_y in pairwise(day_indexes + [max_y]):
                course_name = courses_line.iloc[start_y]
                self.logger.info(f"  > {course_name}")
                group_names = groups_line.iloc[start_y:end_y]
                group_names = group_names[group_names != ''].values
                group_names = list(map(lambda x: format_group_name(x), group_names))

                course_df = week_df.iloc[1:, start_y: end_y]
                course_df = self.refactor_course_df(course_df, group_names)
                separation_by_days[day_name][course_name] = course_df

        return separation_by_days


def get_events_for_course(course_df: pd.DataFrame) -> Iterable[ScheduleEvent]:
    """ Convert course DataFrame (timeslot as index, group name as column name, list of event lines in cell value) to list
    of ScheduleEvents."""
    for timeslot, by_groups in course_df.iterrows():  # type: str, dict
        for name, event_lines in by_groups.items():  # type: str, list[str]
            if not event_lines:
                continue

            start_time, end_time = timeslot.split('-')
            start_time = datetime.datetime.strptime(start_time, '%H:%M').time()
            end_time = datetime.datetime.strptime(end_time, '%H:%M').time()

            cell_event = ScheduleEvent(
                group=name,
                start_time=start_time,
                end_time=end_time
            )

            cell_event.from_cell(event_lines)

            yield cell_event


def convert_separation(
        separation_by_days: dict,
        very_first_date: datetime.date,
        very_last_date: datetime.date,
        logger: logging.Logger
) -> Iterable[ScheduleEvent]:
    now_dtstamp = datetime.datetime.now()

    rrule = get_weekday_rrule(very_last_date)

    logger.info("Converting separation to ScheduleEvent")

    for day_name, separation_by_courses in separation_by_days.items():
        logger.info(f" > Parsing day {day_name}")
        weekday_dtstart = nearest_weekday(very_first_date, weekday_converter[day_name])

        for course_name, course_df in separation_by_courses.items():
            logger.info(f"  > Parsing course {course_name}")
            course_events = get_events_for_course(course_df)

            for course_event in course_events:
                course_event.day = weekday_dtstart
                course_event.recurrence = rrule
                course_event.course = course_name
                course_event.dtstamp = now_dtstamp
                yield course_event


remove_pattern = re.compile(r"\(.*\)")


def format_group_name(dirt_group_name: str) -> str:
    dirt_group_name.replace(' ', '')
    dirt_group_name = dirt_group_name.upper()
    dirt_group_name = remove_pattern.sub('', dirt_group_name)
    dirt_group_name = dirt_group_name.strip()
    return dirt_group_name


def get_weekday_rrule(end_date):
    return {
        'FREQ': 'WEEKLY',
        'INTERVAL': 1,
        'UNTIL': end_date,
    }


def process_target_schedule(target_id) -> Iterable[ScheduleEvent]:
    df = parser.get_clear_df(
        spreadsheet_id=config.SPREADSHEET_ID,
        target_range=config.TARGET_RANGES[target_id],
        target_title=config.TARGET_SHEET_TITLES[target_id]
    )
    separation_by_days = parser.parse_df(df)

    from_date = datetime.datetime.fromisoformat(config.RECURRENCE[target_id]["start"]).date()
    until_date = datetime.datetime.fromisoformat(config.RECURRENCE[target_id]["end"]).date()

    events = convert_separation(
        separation_by_days,
        from_date,
        until_date,
        logger
    )

    return events


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    ignoring_subjects = [Subject.from_str(subject_name) for subject_name in config.IGNORING_SUBJECTS]
    for subject in ignoring_subjects:
        subject.is_ignored = True

    parser = AcademicParser()
    logger = AcademicParser.logger
    all_events = []
    for i in range(len(config.TARGET_RANGES)):
        logger.info(f"Processing target {i}")
        all_events.extend(process_target_schedule(i))

    calendars = {
        "filters": [{
            "title": "Course",
            "alias": "course"
        }],
        "title": "Academic",
        "calendars": []
    }

    directory = PARSER_PATH / config.SAVE_PATH
    json_file = PARSER_PATH / config.SAVE_JSON_PATH

    # replace spaces and dashes with single dash
    replace_spaces_pattern = re.compile(r"[\s-]+")

    all_events = sorted(all_events, key=lambda x: (x.course, x.group))

    logger.info("Writing JSON and iCalendars files...")
    for course_name, course_events in (itertools.groupby(all_events, lambda x: x.course)):
        logger.info(f" > Writing course {course_name}")
        course_path = directory / replace_spaces_pattern.sub('-', course_name)
        course_path.mkdir(parents=True, exist_ok=True)
        for group_name, group_events in (itertools.groupby(course_events, lambda x: x.group)):
            logger.info(f"  > {group_name}...")
            calendar = icalendar.Calendar()
            calendar['prodid'] = '-//one-zero-eight//InNoHassle Calendar'
            calendar['version'] = '2.0'
            calendar['x-wr-calname'] = group_name
            calendar['x-wr-caldesc'] = 'Generated by InNoHassle Calendar'
            calendar['x-wr-timezone'] = config.TIMEZONE

            for group_event in group_events:
                if group_event.subject.is_ignored:
                    logger.info(f"   > Ignoring {group_event.subject.name}")
                    continue

                group_event: ScheduleEvent
                vevent = icalendar.Event(
                    summary=group_event.summary,
                    description=group_event.description,
                    dtstamp=group_event.dtstamp.strftime("%Y%m%dT%H%M%S"),
                    uid=group_event.get_uid(),
                    categories=group_event.subject.name,
                )

                if group_event.location:
                    vevent['location'] = group_event.location

                if specific_date := group_event.flags.only_on_specific_date:
                    dtstart = datetime.datetime.combine(specific_date, group_event.start_time)
                    dtend = datetime.datetime.combine(specific_date, group_event.end_time)
                    vevent['dtstart'] = dtstart.strftime("%Y%m%dT%H%M%S")
                    vevent['dtend'] = dtend.strftime("%Y%m%dT%H%M%S")
                else:
                    vevent['dtstart'] = group_event.dtstart.strftime("%Y%m%dT%H%M%S")
                    vevent['dtend'] = group_event.dtend.strftime("%Y%m%dT%H%M%S")
                    vevent.add('rrule', group_event.recurrence)

                calendar.add_component(vevent)

            file_name = f"{group_name}.ics"
            file_path = course_path / file_name

            calendars["calendars"].append(
                {
                    "name": group_name,
                    "course": course_name,
                    "file": file_path.relative_to(json_file.parent).as_posix()
                }
            )

            with open(file_path, 'wb') as f:
                f.write(calendar.to_ical())

    # create a new .json file with information about calendars
    with open(json_file, "w") as f:

        json.dump(calendars, f, indent=4, sort_keys=True)
