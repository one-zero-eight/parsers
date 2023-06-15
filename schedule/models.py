import datetime
import re
from typing import Optional
from zlib import crc32

import icalendar
from pydantic import BaseModel, validator, Field

from utils import remove_trailing_spaces, symbol_translation

CURRENT_YEAR = datetime.datetime.now().year


class Flags(BaseModel):
    """External flags for the event"""

    only_on_specific_date: bool | datetime.date = False
    """If the event is only on specific date, this flag will be set to that date
    For ex. if the event is only on 2021-09-01, this flag will be set to 2021-09-01"""


class Elective(BaseModel):
    """
    Elective model for ElectivesParserConfig
    """

    alias: str
    """Alias for elective"""
    name: Optional[str]
    """Name of elective"""
    instructor: Optional[str]
    """Instructor of elective"""
    elective_type: Optional[str]
    """Type of elective"""

    @validator("name", "instructor", "elective_type", pre=True)
    def beatify_string(cls: type["Elective"], string: str) -> str:  # noqa
        """Beatify string

        :param string: string to beatify
        :type string: str
        :return: beatified string
        :rtype: str
        """
        if string:
            string = remove_trailing_spaces(string)
            string = string.translate(symbol_translation)
        return string


class ElectiveEvent(BaseModel):
    """
    Elective event model
    """

    elective: Elective
    """ Elective object """
    start: datetime.datetime
    """ Event start time """ ""
    end: datetime.datetime
    """ Event end time """
    location: Optional[str]
    """ Event location """
    description: Optional[str]
    """ Event description """
    event_type: Optional[str]
    """ Event type """
    group: Optional[str] = None
    """ Group to which the event belongs """

    def __hash__(self):
        string_to_hash = str(
            (
                self.elective.alias,
                self.start.isoformat(),
                self.end.isoformat(),
                self.location,
                self.event_type,
                self.group,
            )
        )

        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self: "ElectiveEvent") -> str:
        """
        Get unique identifier for the event

        :return: unique identifier
        :rtype: str
        """
        return "%x@innohassle.ru" % abs(hash(self))

    @property
    def description(self: "ElectiveEvent") -> str:
        """
        Description of the event

        :return: description of the event
        :rtype: str
        """

        r = {
            "Location": self.location,
            "Instructor": self.elective.instructor,
            "Type": self.event_type,
            "Group": self.group,
            "Subject": self.elective.name,
            "Time": f"{self.start.strftime('%H:%M')} - {self.end.strftime('%H:%M')} {self.start.strftime('%d.%m.%Y')}",
        }

        r = {k: v for k, v in r.items() if v}
        return "\n".join([f"{k}: {v}" for k, v in r.items()])

    def get_vevent(self: "ElectiveEvent") -> icalendar.Event:
        """
        Get icalendar event

        :return: icalendar event
        :rtype: icalendar.Event
        """
        vevent = icalendar.Event()
        vevent["summary"] = self.elective.name
        if self.event_type is not None:
            vevent["summary"] += f" ({self.event_type})"
        vevent["dtstart"] = self.start.strftime("%Y%m%dT%H%M%S")
        vevent["dtend"] = self.end.strftime("%Y%m%dT%H%M%S")
        vevent["uid"] = self.get_uid()
        vevent["categories"] = self.elective.name
        vevent["description"] = self.description

        if self.location is not None:
            vevent["location"] = self.location

        return vevent


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

        dirt_name = re.sub(r"\s+\(.*\)\s*$", "", dirt_name)
        dirt_name = re.sub(r"\s+-.*$", "", dirt_name)
        clear_name = re.sub(r"\s+$", "", dirt_name)

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
    dtstamp: Optional[datetime.datetime]
    """Timestamp of the event"""
    location: Optional[str]
    """Location of the event"""
    instructor: Optional[str]
    """Instructor of the event"""
    event_type: Optional[str]
    """Type of the event"""
    recurrence: Optional[list[dict]]
    """Recurrence of the event"""
    flags: Flags = Field(default_factory=Flags)
    """External flags for the event"""
    group: Optional[str]
    """Group for which the event is"""
    course: Optional[str]
    """Course for which the event is"""

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

        only_on = False

        if location:
            # "108 (ONLY ON 14/06)" -> "108", only_on=datetime(6, 14)
            if match := re.search(r"\(ONLY ON (\d+)/(\d+)\)", location):
                location = location[: match.start()].strip()
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

    def get_vevent(self) -> icalendar.Event:
        """Convert event to icalendar.Event

        :return: VEVENT for the event with all fields filled
        :rtype: icalendar.Event
        """
        vevent = icalendar.Event(
            summary=self.summary,
            description=self.description,
            # dtstamp=self.dtstamp.strftime("%Y%m%dT%H%M%S")
            uid=self.get_uid(),
            categories=self.subject.name,
        )

        if self.location:
            vevent["location"] = self.location
        vevent["dtstart"] = self.dtstart.strftime("%Y%m%dT%H%M%S")
        vevent["dtend"] = self.dtend.strftime("%Y%m%dT%H%M%S")

        if specific_date := self.flags.only_on_specific_date:
            dtstart = datetime.datetime.combine(specific_date, self.start_time)
            dtend = datetime.datetime.combine(specific_date, self.end_time)
            vevent["dtstart"] = dtstart.strftime("%Y%m%dT%H%M%S")
            vevent["dtend"] = dtend.strftime("%Y%m%dT%H%M%S")
        elif self.recurrence:
            vevent.add("rrule", self.recurrence)

        return vevent
