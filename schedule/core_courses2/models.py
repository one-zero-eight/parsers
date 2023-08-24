import datetime
import re
from typing import Optional, Literal, Generator
from zlib import crc32

import icalendar
from pandas import isna
from pydantic import BaseModel, Field

from schedule.config_base import CSS3Color
from schedule.core_courses2.config import core_courses_config as config
from schedule.processors.regex import process_spaces
from schedule.utils import *


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
    """Event course"""
    group: str
    """Event acedemic group"""

    start_time: datetime.time
    """Event start time"""
    end_time: datetime.time
    """Event end time"""
    weekday: int = Field(..., ge=0, le=6)
    """Event weekday"""

    starts: datetime.date
    """Event start date"""
    ends: datetime.date
    """Event end date"""

    dtstamp: datetime.datetime
    """Event timestamp"""

    original_value: list[str | None]
    """Original cell values"""

    subject: str
    """Event subject, can be with type in brackets"""
    teacher: Optional[str] = None
    """Event teacher"""
    location: Optional[str] = None
    """Event location"""

    class_type: Optional[Literal["lec", "tut", "lab"]] = None
    """Event class type"""

    only_on: Optional[list[datetime.date]] = None
    """List of dates when event is happening"""

    def __init__(self, **data):
        super().__init__(**data)
        self.process_subject()
        self.process_teacher()
        self.process_location()

    def set_recurrence(self, vevent: icalendar.Event):
        """
        Set recurrence rule and recurrence date for event
        """
        until = datetime.datetime.combine(self.ends, datetime.time.max)

        rrule = icalendar.vRecur(
            {
                "WKST": "MO",
                "FREQ": "WEEKLY",
                "INTERVAL": 1,
                "UNTIL": until,
            }
        )
        vevent.add("rrule", rrule)
        # rdate = icalendar.vDate(self.starts)
        # vevent.add("rdate", rdate)

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

            if re.match(r"^(?:lec|tut|lab)$", inside_brackets, flags=re.IGNORECASE):
                # if inside_brackets is "lec" or "tut" or "lab" then it is class type
                subject = subject.replace(match[0], "", 1)
                self.class_type = inside_brackets.lower()  # type: ignore
            else:
                # if inside_brackets is not "lec" or "tut" or "lab" then it is part of subject
                subject = subject.replace(match[0], f": {inside_brackets.strip()}", 1)
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
        teacher = re.sub(r"(\,\s*)+\,", ",", teacher)
        # remove trailing commas
        teacher = re.sub(r"\s*\,\s*$", "", teacher)
        # remove trailing spaces
        teacher = teacher.strip()
        self.teacher = teacher

    def process_location(self):
        """
        Process location string

        - "303" -> "303"
        - "106/313/314/316/318/320/421" -> "106/313/314/316/318/320/421"
        - "ONLINE" -> "ONLINE"
        - "105 (room #107 on 28/08)" -> "105 (room #107 on 28/08)"
        - "313 (WEEK 1-3) /ONLINE" -> "313 (WEEK 1-3) /ONLINE"

        - "105/ (ONLINE)" -> "105/ONLINE"
        """
        if self.location is None:
            return

        location = self.location
        # sub "ONLINE", "online", "(ONLINE)" and "(online)" with "ONLINE"
        location = re.sub(
            r"\s*\(?ONLINE\)?\s*", "ONLINE", location, flags=re.IGNORECASE
        )
        # replace " and " with comma
        location = re.sub(r"\s+and\s+", ", ", location, flags=re.IGNORECASE)
        # patterns for "only on" information
        location = self.process_only_on(location)
        # patterns for "starts on" information
        location = self.process_starts_on(location)
        # patterns for "starts at" information
        location = self.process_starts_at(location)
        # patterns for "week only" information
        location = self.process_week_only(location)
        # remove spaces near slashes(/)
        location = re.sub(r"\s*\/\s*", "/", location)

        self.location = location

    def process_week_only(self, location):
        """

        - "105 (WEEK 2-3 ONLY)" -> "105", only on weeks 2 and 3 of semester
        - "105 (WEEK 2 ONLY)" -> "105", only on week 2 of semester
        """
        if week_only_m := re.search(
            r"\(?WEEK ([^)]+) ONLY\)?", location, flags=re.IGNORECASE
        ):
            week_only = week_only_m.group(1)
            location = location.replace(week_only_m.group(0), "", 1)
            if "-" in week_only:
                start_week, end_week = week_only.split("-")
                start_week = int(start_week)
                end_week = int(end_week)
                weeks = list(range(start_week, end_week + 1))
            else:
                weeks = [int(week) for week in week_only.split(",")]
            # find dates of weeks
            only_on_dates = []
            start_date = nearest_weekday(self.starts, self.weekday)
            for week in weeks:
                date = start_date + datetime.timedelta(days=(week - 1) * 7)
                only_on_dates.append(date)
            self.only_on = only_on_dates.copy()
        return location

    def process_starts_at(self, location):
        """

        - "STARST AT 16.10" -> starts at 16.10
        - "107 (STARTS AT 10.50)" -> "107", starts at 10.50
        """
        if starts_at_m := re.search(
            r"\(?STARTS AT ([^)]+)\)?", location, flags=re.IGNORECASE
        ):
            starts_at = starts_at_m.group(1).replace(".", ":")
            location = location.replace(starts_at_m.group(0), "", 1)
            hour_and_minute = datetime.datetime.strptime(starts_at, "%H:%M").time()
            self.start_time = hour_and_minute
        return location

    def process_starts_on(self, location):
        """

        - "313 (STARTS FROM 21/09)" -> "313", starts on 21/09
        - "STARTS ON 2/10" -> starts on 2/10
        - "STARTS FROM 21/09" -> starts on 21/09
        """
        if starts_on_m := re.search(
            r"\(?STARTS (?:ON|FROM) ([^)]+)\)?", location, flags=re.IGNORECASE
        ):
            starts_on = starts_on_m.group(1)
            location = location.replace(starts_on_m.group(0), "", 1)
            month_and_day = datetime.datetime.strptime(starts_on, "%d/%m").date()
            date = self.starts.replace(month=month_and_day.month, day=month_and_day.day)
            self.starts = date
        return location

    def process_only_on(self, location):
        """

        - "107 (ONLY ON 8/09, 29/09, 27/10, 17/11)" -> "107", only on 8/09, 29/09, 27/10, 17/11
        - "ONLINE (only on 31/08 and 14/09)" -> "ONLINE", only on 31/08, 14/09
        """

        if only_on_m := re.search(
            r"\(?ONLY ON ([^)]+)\)?", location, flags=re.IGNORECASE
        ):
            only_on = only_on_m.group(1)
            location = location.replace(only_on_m.group(0), "", 1)
            month_and_day = []
            for date_str in re.findall(r"\d{1,2}/\d{1,2}", only_on):
                date = datetime.datetime.strptime(date_str, "%d/%m").date()
                month_and_day.append(date)

            self.only_on = [
                self.starts.replace(month=date.month, day=date.day)
                for date in month_and_day
            ]
        return location

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
        timeslot = (
            f"{self.start_time.strftime('%H:%M')}-{self.start_time.strftime('%H:%M')}"
        )
        return f"{self.course} / {self.group} | {self.subject} {timeslot}"

    def generate_vevents(self) -> Generator[icalendar.Event, None, None]:
        """
        Generate icalendar events

        :return: icalendar events if "only on xx/xx, xx/xx" appeared in location, else one icalendar event
        """

        vevent = icalendar.Event()

        start_of_weekdays = nearest_weekday(self.starts, self.weekday)

        mapping = {
            "summary": self.summary,
            "description": self.description,
            "location": self.location,
            "dtstart": icalendar.vDatetime(
                datetime.datetime.combine(start_of_weekdays, self.start_time)
            ),
            "dtend": icalendar.vDatetime(
                datetime.datetime.combine(start_of_weekdays, self.end_time)
            ),
            "dtstamp": icalendar.vDatetime(self.dtstamp),
            "uid": self.get_uid(),
            "color": self.color,
        }

        for key, value in mapping.items():
            if value:
                vevent.add(key, value)

        if self.only_on is None:
            # recurrence rule
            self.set_recurrence(vevent)
            yield vevent
        else:
            for i, date in enumerate(sorted(self.only_on)):
                x_vevent = vevent.copy()
                x_vevent["dtstart"] = icalendar.vDatetime(
                    datetime.datetime.combine(date, self.start_time)
                )
                x_vevent["dtend"] = icalendar.vDatetime(
                    datetime.datetime.combine(date, self.end_time)
                )
                x_vevent["uid"] = self.get_uid(sequence=str(i))
                yield x_vevent

    @property
    def summary(self):
        return (
            f"{self.subject} ({self.class_type})" if self.class_type else self.subject
        )

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
