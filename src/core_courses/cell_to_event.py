import datetime
import re
import warnings
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.core_courses.config import Target
from src.core_courses.location_parser import Item, parse_location_string
from src.core_courses.parser import CoreCourseCell
from src.logging_ import logger
from src.processors.regex import remove_repeating_spaces_and_trailing_spaces
from src.utils import MOSCOW_TZ, WEEKDAYS


class CoreCourseEvent(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    course: str
    "Academic course"
    group: str
    "Academic group"
    group_student_number: int | None = None
    "Academic group student number"

    start_time: datetime.time
    "Event start time"
    end_time: datetime.time
    "Event end time"
    weekday: int = Field(..., ge=0, le=6)
    "Event weekday"

    starts: datetime.date
    "Event start date"
    ends: datetime.date
    "Event end date"
    dtstamp: datetime.datetime
    "Event timestamp"

    original_value: list[str | None]
    "Original cell values"
    a1: str | None = None
    "A1 coordinates of left-upper cell, may be a range"
    sheet_name: str | None = None
    "Sheet name from which this event was parsed"

    subject: str
    "Event subject"
    teacher: str | None = None
    "Event teacher"
    location: str | None = Field(default=None)
    "Event location (lower priority with respect to location_item)"
    location_item: Item | None = None
    "Parsed location item from location string"
    class_type: Literal["lec", "tut", "lab", "лек", "тут", "лаб"] | None = None
    "Event class type"

    def __init__(self, **data):
        super().__init__(**data)
        self.process_subject()
        self.process_teacher()
        self.process_location()

    def process_subject(self):
        """Process subject string

        - "Mathematical Analysis I (lec)" -> "Mathematical Analysis I" + lec as type
        - "Philosophy II (Introduction to AI) (lec)" -> "Philosophy II: Introduction to AI" + lec as type
        - "Analytical Geometry and Linear Algebra I" -> "Analytical Geometry and Linear Algebra I"
        """

        subject = self.subject
        matches = re.finditer(r"\((.+?)\)", subject)
        for match in matches:
            inside_brackets = match.group(1)

            if re.match(r"^(?:lec|tut|lab|тут|лек|лаб)$", inside_brackets, flags=re.IGNORECASE):
                # if inside_brackets is "lec" or "tut" or "lab" then it is class type
                subject = subject.replace(match[0], "", 1)
                self.class_type = inside_brackets.lower()  # type: ignore
            else:
                # if inside_brackets is not "lec" or "tut" or "lab" then it is part of subject
                subject = subject.replace(match[0], f": {inside_brackets.strip()}", 1)

        # remove whitespaces before colons(:)
        subject = re.sub(r"\s*:\s*", ": ", subject)
        subject = remove_repeating_spaces_and_trailing_spaces(subject)
        self.subject = subject

    def process_teacher(self):
        """Process teacher string

        - "Ivan Ivanov" -> "Ivan Ivanov"
        - "Maria Razmazina/David Orok" -> "Maria Razmazina, David Orok"
        - "Georgiy Gelvanovsky,Rabab Marouf, Ruslan Saduov, Oksana Zhirosh"
          -> "Georgiy Gelvanovsky, Rabab Marouf, Ruslan Saduov, Oksana Zhirosh"
        - "M. Reza Bahrami" -> "M. Reza Bahrami"
        """
        if self.teacher is None:
            return

        teacher = self.teacher
        # remove spaces before and after commas(,) and slashes(/) and replace them with comma(,)
        teacher = re.sub(r"\s*[,/]\s*", ",", teacher)
        # remove multiple commas in a row
        teacher = re.sub(r"(,\s*)+,", ",", teacher)
        # remove trailing commas
        teacher = re.sub(r"\s*,\s*$", "", teacher)
        # remove trailing spaces
        teacher = teacher.strip()
        self.teacher = teacher

    def process_location(self):
        """
        Process location string, See corresponding tests in tests/test_location_strings.py
        """
        if self.location is None:
            return
        location = self.location
        # Upper case location
        location = location.upper()
        # replace " and " with comma
        location = re.sub(r"\s+AND\s+", ", ", location)
        self.location = location

        if not re.match(r"ELECTIVE COURSES? ON PHYSICAL EDUCATION", location):  # no need to parse this location
            self.location_item = parse_location_string(location)

            if self.location_item is None:
                warnings.warn(f"Location `{location}` is not parsed properly")

    def __str__(self):
        timeslot = f"{self.start_time.strftime('%H:%M')}-{self.start_time.strftime('%H:%M')}"
        return f"{self.course} / {self.group} | {self.subject} {timeslot}"


def convert_cell_to_event(
    cell: CoreCourseCell,
    weekday: str,
    timeslot: tuple[datetime.time, datetime.time],
    course: str,
    group: str,
    target: Target,
) -> CoreCourseEvent | None:
    """
    Convert cell to event
    """
    weekday_int = WEEKDAYS.index(weekday)
    start_time, end_time = timeslot

    try:
        subject = teacher = location = None

        match cell.value:
            case None, subject, None:
                pass
            case subject, teacher, location:
                pass
            case _:
                raise ValueError(f"Unknown value: {cell.value}")

        starts = target.start_date
        ends = target.end_date

        def preprocess_group(value: str) -> tuple[str, int | None]:
            """
            Process group name

            - "M21-DS(16)" -> "M21-DS"
            - "M22-TE-01 (10)" -> "M22-TE-01"
            - "B20-SD-02 (29)" -> "B20-SD-02"
            """
            student_number = None
            # Match (G\d+) or (\d+) at the end
            if student_number_m := re.search(r"\(?(G\d+|\d+)\)?\s*$", value):
                match_text = student_number_m.group(1)
                if match_text.startswith("G"):
                    student_number = int(match_text[1:])
                else:
                    student_number = int(match_text)
                value = value.replace(student_number_m.group(0), "").strip()
            return value, student_number

        group, group_student_number = preprocess_group(group)

        for override in target.override:
            if group in override.groups or course in override.courses:
                starts = override.start_date
                ends = override.end_date
                break

        event = CoreCourseEvent(
            start_time=start_time,
            end_time=end_time,
            dtstamp=datetime.datetime.combine(target.start_date, datetime.time.min, tzinfo=MOSCOW_TZ),
            starts=starts,
            ends=ends,
            weekday=weekday_int,
            course=course,
            group=group,
            group_student_number=group_student_number,
            original_value=cell.value,
            a1=cell.a1,
            subject=subject,
            teacher=teacher,
            location=location,
        )
        return event
    except ValueError:
        logger.error(f"Error parsing cell {cell.value} for {course} {group} {weekday} {timeslot}", exc_info=True)
        return None
