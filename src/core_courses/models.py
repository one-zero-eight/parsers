import datetime
import re
import warnings
from typing import Optional, Literal, Generator
from zlib import crc32

import icalendar
from pandas import isna
from pydantic import BaseModel, Field

from src.config_base import CSS3Color
from src.core_courses.config import core_courses_config as config
from src.core_courses.location_parser import Item, parse_location_string
from src.logging_ import logger
from src.processors.regex import process_spaces
from src.utils import *


class CoreCourseCell:
    """Notes for the event"""

    value: list[str | None]
    """Cell values"""

    def __init__(self, value: list[str | None]):
        if len(value) == 3:
            self.value = [None if (isna(x)) else x for x in value]
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
            target: config.Target,
            return_none: bool = False,
    ) -> Optional["CoreCourseEvent"]:
        """
        Get event from cell

        :return: event from cell
        :rtype: Optional[CoreCourseCell]
        """
        weekday_int = config.WEEKDAYS.index(weekday)
        start_time, end_time = timeslot
        cell_info = self.parse_value_into_parts()

        try:
            event = CoreCourseEvent(
                start_time=start_time,
                end_time=end_time,
                dtstamp=datetime.datetime.combine(target.start_date, datetime.time.min),
                starts=target.start_date,
                ends=target.end_date,
                weekday=weekday_int,
                course=self.preprocess_course(course),
                group=self.preprocess_group(group),
                original_value=self.value,
                **cell_info,
            )
            return event
        except ValueError:
            if return_none:
                return None
            raise

    @classmethod
    def preprocess_course(cls, value: str) -> str:
        """
        Process course name

        :param value: course name
        :type value: str
        :return: processed course name
        :rtype: str
        """
        return value

    @classmethod
    def preprocess_group(cls, value: str) -> str:
        """
        Process group name

        :param value: group name
        :type value: str
        :return: processed group name
        :rtype: str

        - "M21-DS(16)" -> "M21-DS"
        - "M22-TE-01 (10)" -> "M22-TE-01"
        - "B20-SD-02 (29)" -> "B20-SD-02"
        """
        return re.sub(r"\s*\(\d+\)\s*$", "", value)

    def parse_value_into_parts(self) -> dict[str, ...]:
        """
        Process cell value

        :return: processed value
        :rtype: dict[str, ...]
        """

        match self.value:
            case None, subject, None:
                return {"subject": subject}
            case subject, teacher, location:
                return {
                    "subject": subject,
                    "teacher": teacher,
                    "location": location,
                }


class CoreCourseEvent(BaseModel):
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
    teacher: Optional[str] = None
    "Event teacher"
    location: Optional[str] = None
    "Event location (lower priority with respect to location_item)"
    location_item: Item | None = None
    "Parsed location item from location string"
    class_type: Optional[Literal["lec", "tut", "lab", "лек", "тут", "лаб"]] = None
    "Event class type"

    def __init__(self, **data):
        super().__init__(**data)
        self.process_subject()
        self.process_teacher()
        self.process_location()

    def every_week_rule(self) -> icalendar.vRecur:
        """
        Set recurrence rule and recurrence date for event
        """
        until = datetime.datetime.combine(self.ends, datetime.time.min)

        rrule = icalendar.vRecur(
            {
                "WKST": "MO",
                "FREQ": "WEEKLY",
                "INTERVAL": 1,
                "UNTIL": until,
            }
        )
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
        return sequence + ("-%x@innohassle.ru" % abs(hash(self)))

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

        def preprocess_on_and_weeks_on(item: Item):
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

        if self.location_item:
            if self.location_item.NEST:
                # shaking:
                # if NEST does not have on or weeks_on then it is should be dropped from the list and
                # all properties should be moved to the parent
                new_nest = []
                for item in self.location_item.NEST:
                    preprocess_on_and_weeks_on(item)
                    if not item.on:
                        logger.info(
                            f"NEST item {item} has not on or weeks_on, its properties will be propagated to parent"
                        )
                        if item.location:
                            if self.location_item.location:
                                warnings.warn(
                                    "Both parent and NEST have location, NEST properties will be skipped "
                                    f"{item.location}, {self.location_item.location}"
                                )  # TODO: handle case "421 (316 FROM 31/10)"
                                continue
                            else:
                                self.location_item.location = item.location

                        if item.starts_from:
                            if self.location_item.starts_from:
                                warnings.warn(
                                    "Both parent and NEST have starts_from, NEST starts_from will be skipped "
                                    f"{item.starts_from}, {self.location_item.starts_from}"
                                )
                            else:
                                self.location_item.starts_from = item.starts_from

                        if item.starts_at:
                            if self.location_item.starts_at:
                                warnings.warn(
                                    "Both parent and NEST have starts_at, NEST starts_at will be skipped "
                                    f"{item.starts_at}, {self.location_item.starts_at}"
                                )
                            else:
                                self.location_item.starts_at = item.starts_at

                        if item.till:
                            if self.location_item.till:
                                warnings.warn(
                                    "Both parent and NEST have till, NEST till will be skipped "
                                    f"{item.till}, {self.location_item.till}"
                                )
                            else:
                                self.location_item.till = item.till

                    else:
                        new_nest.append(item)

                self.location_item.NEST = new_nest

            if self.location_item.location:
                self.location = self.location_item.location
            if self.location_item.starts_from:
                self.starts = self.location_item.starts_from
            if self.location_item.starts_at:
                # move event start time to location starts_at time keeping same duration
                _start_time = datetime.datetime.combine(self.starts, self.start_time)
                _end_time = datetime.datetime.combine(self.starts, self.end_time)
                duration = _end_time - _start_time
                self.start_time = self.location_item.starts_at
                self.end_time = (datetime.datetime.combine(self.starts, self.start_time) + duration).time()
            if self.location_item.till:
                self.end_time = self.location_item.till
            preprocess_on_and_weeks_on(self.location_item)

        start_of_weekdays = nearest_weekday(self.starts, self.weekday)
        dtstart = datetime.datetime.combine(start_of_weekdays, self.start_time)
        dtend = datetime.datetime.combine(start_of_weekdays, self.end_time)
        duration = dtend - dtstart

        mapping = {
            "summary": self.summary,
            "description": self.description,
            "location": self.location,
            "dtstamp": icalendar.vDatetime(self.dtstamp),
            "uid": self.get_uid(),
            "color": self.color,
        }

        vevent = icalendar.Event()

        for key, value in mapping.items():
            if value:
                vevent.add(key, value)

        if self.location_item and self.location_item.on:  # only on specific dates, not every week
            rdates = [dtstart.replace(day=on.day, month=on.month) for on in self.location_item.on]
            vevent.add("rdate", rdates)
            # dtstart and dtend should be adapted
            dtstart = dtstart.replace(day=self.location_item.on[0].day, month=self.location_item.on[0].month)
            dtend = dtend.replace(day=self.location_item.on[0].day, month=self.location_item.on[0].month)
        else:  # every week at the same time
            vevent["rrule"] = self.every_week_rule()

        vevent["dtstart"] = icalendar.vDatetime(dtstart)
        vevent["dtend"] = icalendar.vDatetime(dtend)

        if not self.location_item or not self.location_item.NEST:  # Simple case, only one event
            yield vevent
            return

        # NEST
        if vevent.has_key("rrule"):  # event with rrule
            for i, item in enumerate(self.location_item.NEST):
                if not item.on:
                    warnings.warn(f"NEST item {item} has no on, it is not possible to create event")
                    continue
                # override specific recurrence entry
                for j, on in enumerate(item.on):
                    vevent_copy = vevent.copy()
                    _recurrence_id = dtstart.replace(day=on.day, month=on.month)
                    vevent_copy["recurrence-id"] = icalendar.vDatetime(_recurrence_id)
                    vevent_copy["sequence"] = i + j
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

            for i, item in enumerate(self.location_item.NEST):
                if not item.on:
                    warnings.warn(f"NEST item {item} has no on, it is not possible to create event")
                    continue

                vevent_copy = vevent.copy()
                vevent_copy["uid"] = self.get_uid(sequence=str(i))
                vevent_copy.pop("rdate")
                rdates = [dtstart.replace(day=on.day, month=on.month) for on in item.on]
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
            "Time": f"{self.start_time.strftime('%H:%M')} - {self.end_time.strftime('%H:%M')}",
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
