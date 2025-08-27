import datetime
import re
import warnings
from collections.abc import Generator
from datetime import UTC
from typing import Literal, Optional
from zlib import crc32

import icalendar
from pandas import isna
from pydantic import BaseModel, ConfigDict, Field

from src.constants import WEEKDAYS, CSS3Color
from src.core_courses.config import Target
from src.core_courses.location_parser import Item, parse_location_string
from src.logging_ import logger
from src.processors.regex import process_spaces
from src.utils import *
from src.utils import MOSCOW_TZ


class CoreCourseCell:
    """Notes for the event"""

    value: list[str | None]
    """Cell values"""

    def __init__(self, value: list[str | None]):
        if len(value) == 3:
            self.value = [None if (isna(x)) else x for x in value]
        elif len(value) == 1:
            self.value = [None if (isna(value[0])) else value[0]] + [None] * 2
        else:
            raise ValueError(f"Length of value must be 3. {value}")

    def __repr__(self):
        return self.value.__repr__()

    def get_event(
        self: "CoreCourseCell",
        weekday: str,
        timeslot: tuple[datetime.time, datetime.time],
        course: str,
        group: str,
        target: Target,
    ) -> Optional["CoreCourseEvent"]:
        """
        Convert cell to event
        """
        weekday_int = WEEKDAYS.index(weekday)
        start_time, end_time = timeslot
        cell_info = self.parse_value_into_parts()

        try:
            starts = target.start_date
            ends = target.end_date

            group = self.preprocess_group(group)

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
                original_value=self.value,
                **cell_info,
            )
            return event
        except ValueError:
            logger.error(f"Error parsing cell {self.value} for {course} {group} {weekday} {timeslot}", exc_info=True)
            return None

    @classmethod
    def preprocess_group(cls, value: str) -> str:
        """
        Process group name

        - "M21-DS(16)" -> "M21-DS"
        - "M22-TE-01 (10)" -> "M22-TE-01"
        - "B20-SD-02 (29)" -> "B20-SD-02"
        """
        return re.sub(r"\s*\(\d+\)\s*$", "", value)

    def parse_value_into_parts(self) -> dict[str, str | None]:
        match self.value:
            case None, subject, None:
                return {"subject": subject}
            case subject, teacher, location:
                return {
                    "subject": subject,
                    "teacher": teacher,
                    "location": location,
                }
            case _:
                raise ValueError(f"Unknown value: {self.value}")


class CoreCourseEvent(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    course: str
    "Event course"
    group: str
    "Event acedemic group"

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

    subject: str
    "Event subject, can be with type in brackets"
    teacher: str | None = None
    "Event teacher"
    location: str | None = Field(default=None)
    "Event location (lower priority with respect to location_item)"
    location_item: Item | None = None
    "Parsed location item from location string"
    class_type: Literal["lec", "tut", "lab", "лек", "тут", "лаб"] | None = None
    "Event class type"

    _sequence_number: int = -1

    def __init__(self, **data):
        super().__init__(**data)
        self.process_subject()
        self.process_teacher()
        self.process_location()

    def every_week_rule(self) -> icalendar.vRecur:
        """
        Set recurrence rule and recurrence date for event
        """
        until = datetime.datetime.combine(self.ends, datetime.time.min).astimezone(UTC)

        rrule = icalendar.vRecur({"WKST": "MO", "FREQ": "WEEKLY", "INTERVAL": 1, "UNTIL": until})
        return rrule

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
        subject = process_spaces(subject)
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

    def __hash__(self):
        string_to_hash = str(
            (
                "core courses",
                self.course,
                self.start_time.isoformat(),
                self.end_time.isoformat(),
                self.starts.isoformat(),
                self.ends.isoformat(),
                self.weekday,
                *self.original_value,
            )
        )

        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self: "CoreCourseEvent", sequence: str = "x") -> str:
        """
        Get unique identifier for the event

        :return: unique identifier
        :rtype: str
        """
        return sequence + f"-{abs(hash(self)):x}@innohassle.ru"

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        timeslot = f"{self.start_time.strftime('%H:%M')}-{self.start_time.strftime('%H:%M')}"
        return f"{self.course} / {self.group} | {self.subject} {timeslot}"

    def generate_vevents(self) -> Generator[icalendar.Event, None, None]:
        """
        Generate icalendar events

        :return: icalendar events if "only on xx/xx, xx/xx" appeared in location, else one icalendar event
        """

        if not self.location_item:
            start_of_weekdays = nearest_weekday(self.starts, self.weekday)
            dtstart = datetime.datetime.combine(start_of_weekdays, self.start_time, tzinfo=MOSCOW_TZ)
            dtend = datetime.datetime.combine(start_of_weekdays, self.end_time, tzinfo=MOSCOW_TZ)
            mapping = {
                "summary": self.summary,
                "description": self.description,
                "location": self.location,
                "dtstamp": icalendar.vDatetime(self.dtstamp),
                "uid": self.get_uid(),
                "color": self.color,
                "rrule": self.every_week_rule(),
                "dtstart": icalendar.vDatetime(dtstart),
                "dtend": icalendar.vDatetime(dtend),
            }
            vevent = icalendar.Event()
            for key, value in mapping.items():
                if value:
                    vevent.add(key, value)
            yield vevent
            return

        def convert_weeks_on_to_only_on(item: Item):
            if item.on_weeks:
                on = []
                for week in item.on_weeks:
                    on_date = nearest_weekday(self.starts, self.weekday) + datetime.timedelta(weeks=week - 1)
                    on.append(on_date)
                if item.on:
                    item.on.extend(on)
                elif on:
                    item.on = on
            if item.on:
                item.on = sorted(set(item.on))

        location_item = self.location_item
        location = location_item.location or self.location
        starts = location_item.starts_from or self.starts
        start_time = self.start_time
        end_time = self.end_time
        # move event start time to location starts_at time keeping same duration
        if location_item.starts_at:
            _start_time = datetime.datetime.combine(starts, start_time, tzinfo=MOSCOW_TZ)
            _end_time = datetime.datetime.combine(starts, end_time, tzinfo=MOSCOW_TZ)
            duration = _end_time - _start_time
            start_time = location_item.starts_at
            end_time = (datetime.datetime.combine(starts, start_time, tzinfo=MOSCOW_TZ) + duration).time()
        if location_item.till:
            end_time = location_item.till

        convert_weeks_on_to_only_on(location_item)
        start_of_weekdays = nearest_weekday(starts, self.weekday)
        dtstart = datetime.datetime.combine(start_of_weekdays, start_time, tzinfo=MOSCOW_TZ)
        dtend = datetime.datetime.combine(start_of_weekdays, end_time, tzinfo=MOSCOW_TZ)
        duration = dtend - dtstart

        mapping = {
            "summary": self.summary,
            "description": self.description,
            "location": location,
            "dtstamp": icalendar.vDatetime(self.dtstamp),
            "uid": self.get_uid(),
            "color": self.color,
        }

        vevent = icalendar.Event()

        for key, value in mapping.items():
            if value:
                vevent.add(key, value)

        if location_item.on:  # only on specific dates, not every week
            rdates = [
                dtstart.replace(day=on.day, month=on.month) for on in location_item.on if self.starts <= on <= self.ends
            ]
            if not rdates:
                logger.warning(f"Event {self} has no rdates")
                return
            vevent.add("rdate", rdates)
            # dtstart and dtend should be adapted
            dtstart = dtstart.replace(day=location_item.on[0].day, month=location_item.on[0].month)
            dtend = dtend.replace(day=location_item.on[0].day, month=location_item.on[0].month)
        else:  # every week at the same time
            vevent.add("rrule", self.every_week_rule())

        vevent["dtstart"] = icalendar.vDatetime(dtstart)
        vevent["dtend"] = icalendar.vDatetime(dtend)

        # check for item.except_ and add exdate if needed
        if location_item.except_:
            exdates = [dtstart.replace(day=on.day, month=on.month) for on in location_item.except_]
            vevent.add("exdate", exdates)

        nested_on = []
        extra_nested = []
        if location_item.NEST:
            for item in location_item.NEST:
                convert_weeks_on_to_only_on(item)
                if item.on:
                    nested_on.append(item)
                else:
                    logger.info(f"Root Item: {location_item}, {self.original_value}")
                    extra_nested.append(item)

        if not (nested_on or extra_nested):  # Simple case, only one event
            yield vevent
            return

        if extra_nested:  # TODO: Handle '421 (316 FROM 31/10)' case
            warnings.warn(f"Extra nested is not implemented yet\nItem({location_item})")

        # NEST
        if vevent.has_key("rrule"):  # event with rrule
            seq = 0

            for i, item in enumerate(nested_on):
                # override specific recurrence entry
                for j, on in enumerate(item.on):
                    if self.starts > on or on > self.ends:
                        continue
                    seq += 1
                    vevent_copy = vevent.copy()
                    _recurrence_id = dtstart.replace(day=on.day, month=on.month)
                    vevent_copy["recurrence-id"] = icalendar.vDatetime(_recurrence_id)
                    vevent_copy["sequence"] = seq
                    vevent_copy.pop("rrule")
                    # adapt dtstart and dtend
                    _dtstart = dtstart.replace(day=on.day, month=on.month)
                    _dtend = dtend.replace(day=on.day, month=on.month)
                    vevent_copy["dtstart"] = icalendar.vDatetime(_dtstart)
                    vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
                    if item.location:
                        vevent_copy["location"] = item.location
                    if item.starts_at:
                        _dtstart = _dtstart.replace(hour=item.starts_at.hour, minute=item.starts_at.minute)
                        _dtend = _dtstart + duration
                        vevent_copy["dtstart"] = icalendar.vDatetime(_dtstart)
                        vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
                    if item.till:
                        _dtend = _dtend.replace(hour=item.till.hour, minute=item.till.minute)
                        vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
                    yield vevent_copy
            yield vevent
        else:  # just a single event on specific dates
            yield vevent

            for i, item in enumerate(nested_on):
                vevent_copy = vevent.copy()
                vevent_copy["uid"] = self.get_uid(sequence=str(i))
                vevent_copy.pop("rdate")
                rdates = [
                    dtstart.replace(day=on.day, month=on.month) for on in item.on if self.starts <= on <= self.ends
                ]
                if not rdates:
                    continue
                vevent_copy.add("rdate", rdates)
                # adapt dtstart and dtend
                _dtstart = dtstart.replace(day=item.on[0].day, month=item.on[0].month)
                _dtend = dtend.replace(day=item.on[0].day, month=item.on[0].month)
                vevent_copy["dtstart"] = icalendar.vDatetime(_dtstart)
                vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
                if item.location:
                    vevent_copy["location"] = item.location
                if item.starts_at:
                    _dtstart = _dtstart.replace(hour=item.starts_at.hour, minute=item.starts_at.minute)
                    _dtend = _dtstart + duration
                    vevent_copy["dtstart"] = icalendar.vDatetime(_dtstart)
                    vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
                if item.till:
                    _dtend = _dtend.replace(hour=item.till.hour, minute=item.till.minute)
                    vevent_copy["dtend"] = icalendar.vDatetime(_dtend)

                yield vevent_copy

    @property
    def summary(self):
        return f"{self.subject} ({self.class_type})" if self.class_type else self.subject

    @property
    def description(self):
        r = {
            # "Location": self.location,
            "Instructor": self.teacher,
            # "Type": self.class_type,
            "Group": self.group,
            "Course": self.course,
            # "Subject": self.subject,
        }

        r = {k: v for k, v in r.items() if v}
        if r:
            return "\n".join([f"{k}: {v}" for k, v in r.items()])

    @property
    def color(self):
        color_count = len(CSS3Color)
        to_hash_ = self.subject.encode("utf-8")
        hash_ = crc32(to_hash_)
        return CSS3Color.get_by_index(hash_ % color_count)
