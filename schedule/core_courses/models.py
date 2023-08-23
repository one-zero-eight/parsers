import datetime
import re
from typing import Optional
from zlib import crc32

import icalendar
from pydantic import BaseModel, Field, validator

from schedule.config_base import CSS3Color
from schedule.processors.regex import (
    process_only_on,
    process_desc_in_parentheses,
    process_spaces,
)


class Subject(BaseModel):
    """
    Subject model for the schedule parser
    """

    name: str
    """Name of the subject
    For ex. "Elective courses on Physical Education"
    """
    is_ignored: bool = False
    """Is the current subject will be ignored by the parser
    For ex. for the "Elective courses on Physical Education"
    """

    @classmethod
    def from_str(cls: type["Subject"], dirt_name: str) -> "Subject":
        """
        Create Subject instance from name of the subject
        Note: uses flyweight pattern to prevent copies

        :param dirt_name: name from the table as it is. For ex.: "Software Project  (lec)                  "
        :type dirt_name: str
        :return: Subject instance
        :rtype: Subject
        """

        dirt_name = re.sub(r"\s*\(.*\)\s*", "", dirt_name)
        dirt_name = re.sub(r"\s*-.*$", "", dirt_name)
        clear_name = process_spaces(dirt_name)

        if clear_name not in cls.__instances__:
            cls.__instances__[clear_name] = cls(name=clear_name)
        return cls.__instances__[clear_name]

    @classmethod
    def get(cls: type["Subject"], name: str) -> Optional["Subject"]:
        """
        Get instance by name

        :param name: name of the subject
        :type name: str
        :return: Subject instance if it is exists
        :rtype: Optional[Subject]
        """
        return cls.__instances__.get(name)

    @classmethod
    def get_all(cls: type["Subject"]) -> list["Subject"]:
        """
        Get all instances of the Subject

        :return: list of Subject instances
        :rtype: list[Subject]
        """
        return list(cls.__instances__.values())

    @property
    def color(self: "Subject") -> CSS3Color:
        """
        Get color for the subject

        :return: color for the subject
        """

        color_count = len(CSS3Color)
        hash_ = crc32(self.name.encode("utf-8"))
        return CSS3Color.get_by_index(hash_ % color_count)

    __instances__: dict[str, "Subject"] = {}
    """Flyweight pattern storage"""


class ScheduleEvent(BaseModel):
    """Schedule event model for the schedule parser"""

    subject: Optional[Subject]
    """Subject of the event"""
    start_time: Optional[datetime.time]
    """Start time of the event"""
    end_time: Optional[datetime.time]
    """End time of the event"""
    day: Optional[datetime.date]
    """Day of the event"""
    dtstamp: Optional[datetime.date]
    """Timestamp of the event"""
    location: Optional[str]
    """Location of the event"""
    instructor: Optional[str]
    """Instructor of the event"""
    event_type: Optional[str]
    """Type of the event"""
    recurrence: Optional[icalendar.vRecur]
    """Recurrence of the event"""
    flags: "Flags" = Field(default_factory=lambda: Flags())
    """External flags for the event"""
    group: Optional[str]
    """Group for which the event is"""
    course: Optional[str]
    """Course for which the event is"""

    class Config:
        validate_assignment = True

    @validator("recurrence", pre=True, always=True)
    def convert_to_ical(cls, v: dict | None) -> Optional[icalendar.vRecur]:
        if isinstance(v, dict):
            v = icalendar.vRecur(**v)
        return v

    @property
    def summary(self: "ScheduleEvent") -> str:
        """
        Summary of the event

        :return: summary of the event
        :rtype: str
        """
        r = f"{self.subject.name}"
        if self.event_type:
            r += f" ({self.event_type})"
        return r

    @property
    def description(self: "ScheduleEvent") -> str:
        """
        Description of the event

        :return: description of the event
        :rtype: str
        """
        r = {
            "Location": self.location,
            "Instructor": self.instructor,
            "Type": self.event_type,
            "Group": self.group,
            "Subject": self.subject.name,
            "Time": f"{self.start_time.strftime('%H:%M')} - {self.end_time.strftime('%H:%M')}",
        }

        r = {k: v for k, v in r.items() if v}
        return "\n".join([f"{k}: {v}" for k, v in r.items()])

    @property
    def dtstart(self: "ScheduleEvent") -> datetime.datetime:
        """
        Datetime of the start of the event

        :return: datetime of the start of the event
        :rtype: datetime.datetime
        """
        return datetime.datetime.combine(self.day, self.start_time)

    @property
    def dtend(self: "ScheduleEvent") -> datetime.datetime:
        """
        Datetime of the end of the event

        :return: datetime of the end of the event
        :rtype: datetime.datetime
        """
        return datetime.datetime.combine(self.day, self.end_time)

    def __hash__(self: "ScheduleEvent") -> int:
        """
        Hash of the event

        :return: hash of the event
        :rtype: int
        """
        string_to_hash = str(
            (
                self.subject.name,
                self.event_type,
                self.start_time.isoformat(),
                self.end_time.isoformat(),
                self.group,
                self.day.isoformat(),
            )
        )

        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self: "ScheduleEvent") -> str:
        """
        Get unique id of the event

        :return: unique id of the event
        :rtype: str
        """
        return "%x@innohassle.ru" % abs(hash(self))

    def __eq__(self: "ScheduleEvent", other: "ScheduleEvent") -> bool:
        """
        Check if the event is equal to other event

        :param other: other event
        :type other: ScheduleEvent
        :return: is the event is equal to other event
        :rtype: bool
        """
        return (
            self.subject == other.subject
            and self.event_type == other.event_type
            and self.start_time == other.start_time
            and self.end_time == other.end_time
            and self.group == other.group
        )

    def from_cell(self: "ScheduleEvent", lines: list[str]) -> None:
        """
        Parse event from cell

        :param lines: list of lines in the cell
        :type lines: list[str]
        :return: None
        :rtype: None
        """
        # lines = [pretty for line in lines if (pretty := remove_trailing_spaces(line))]
        iterator = filter(None, lines)
        _title = next(iterator, None)
        subject = Subject.from_str(_title)
        instructor = next(iterator, None)
        location = next(iterator, None)
        event_type = None
        only_on = None

        if location and (r := process_only_on(location)):
            location, only_on = r

        if _title and (r := process_desc_in_parentheses(_title)):
            _, event_type = r

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

    def get_vevents(self) -> list[icalendar.Event]:
        """Convert event to icalendar.Event

        :return: VEVENT for the event with all fields filled
        :rtype: icalendar.Event
        """

        vevents = []
        dtstart = datetime.datetime.combine(self.day, self.start_time)
        dtend = datetime.datetime.combine(self.day, self.end_time)
        vevent = icalendar.Event(
            summary=self.summary,
            description=self.description,
            uid=self.get_uid(),
            categories=self.subject.name,
            dtstart=icalendar.vDatetime(dtstart),
            dtend=icalendar.vDatetime(dtend),
        )

        # if self.dtstamp:
        #     vevent["dtstamp"] = self.dtstamp.strftime("%Y%m%dT%H%M%S")

        if hasattr(self.subject, "color"):
            vevent["color"] = self.subject.color

        if self.location:
            vevent["location"] = self.location

        if specific_date := self.flags.only_on_specific_date:
            for i, date in enumerate(specific_date):
                date = date.replace(year=self.day.year)
                dtstart = datetime.datetime.combine(date, self.start_time)
                dtend = datetime.datetime.combine(date, self.end_time)
                _vevent = vevent.copy()
                _vevent["dtstart"] = icalendar.vDatetime(dtstart)
                _vevent["dtend"] = icalendar.vDatetime(dtend)
                _vevent["uid"] = f"{i}_" + _vevent["uid"]
                vevents.append(_vevent)
        elif self.recurrence:
            _vevent = vevent.copy()
            _vevent["rrule"] = self.recurrence
            vevents.append(_vevent)
        else:
            vevents.append(vevent)

        return vevents


class Flags(BaseModel):
    """External flags for the event"""

    only_on_specific_date: list[datetime.date] | None = None
    """If the event is only on specific date, this flag will be set to that date
    For ex. if the event is only on 2021-09-01, this flag will be set to 2021-09-01"""
