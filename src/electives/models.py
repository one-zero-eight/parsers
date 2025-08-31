import datetime
import re
from collections.abc import Generator
from typing import Any
from zlib import crc32

import icalendar
from pydantic import BaseModel, Field, field_validator

from src.processors.regex import process_spaces
from src.utils import MOSCOW_TZ, get_color


class Elective(BaseModel):
    """
    Elective model for ElectivesParserConfig
    """

    alias: str
    """Alias for elective"""
    name: str | None = None
    """Name of elective"""
    instructor: str | None = None
    """Instructor of elective"""
    elective_type: str | None = None
    """Type of elective"""

    @field_validator("name", "instructor", "elective_type", mode="before")
    @classmethod
    def beatify_string(cls: type["Elective"], string: str) -> str:
        """Beatify string

        :param string: string to beatify
        :type string: str
        :return: beatified string
        :rtype: str
        """
        if string:
            string = process_spaces(string)
        return string


class ElectiveCell(BaseModel):
    original: list[str]
    """ Original cell value """

    class Occurrence(BaseModel):
        """Occurrence of the elective"""

        original: str
        """ Original occurrence value """
        elective: Elective | None = None
        """ Elective object """
        location: str | None = None
        """ Location of the elective """
        group: str | None = None
        """ Group to which the elective belongs """
        class_type: str | None = None
        """ Type of the class(leture, seminar, etc.) """
        starts_at: datetime.time | None = None
        """ Time when the elective starts (modificator) """
        ends_at: datetime.time | None = None
        """ Time when the elective ends (modificator) """

        def __init__(self, **data: Any):
            """
            Process cell value

            - GAI (lec) online
            - PHL 101
            - PMBA (lab) (Group 1) 313
            - GDU 18:00-19:30 (lab) 101
            - OMML (18:10-19:50) 312
            - PGA 300
            - IQC (17:05-18:35) online
            - SMP online
            - ASEM (starts at 18:05) 101
            """

            from src.electives.config import electives_config as config

            super().__init__(**data)

            string = self.original.strip()
            # just first word as elective
            splitter = string.split(" ")
            elective_alias = splitter[0]
            self.elective = next(elective for elective in config.electives if elective.alias == elective_alias)
            string = " ".join(splitter[1:])
            # find time xx:xx-xx:xx
            if timeslot_m := re.search(r"\(?(\d{2}:\d{2})-(\d{2}:\d{2})\)?", string):
                self.starts_at = datetime.datetime.strptime(timeslot_m.group(1), "%H:%M").time()
                self.ends_at = datetime.datetime.strptime(timeslot_m.group(2), "%H:%M").time()
                string = string.replace(timeslot_m.group(0), "")

            # find starts at xx:xx
            if timeslot_m := (
                re.search(r"\(?starts at (\d{2}:\d{2})\)?", string) or re.search(r"\(?начало в (\d{2}:\d{2})\)?", string)
            ):
                self.starts_at = datetime.datetime.strptime(timeslot_m.group(1), "%H:%M").time()
                string = string.replace(timeslot_m.group(0), "")
            
            # find ends at xx:xx
            if timeslot_m := re.search(r"\(?ends at (\d{2}:\d{2})\)?", string) or re.search(r"\(?конец в (\d{2}:\d{2})\)?", string):
                self.ends_at = datetime.datetime.strptime(timeslot_m.group(1), "%H:%M").time()
                string = string.replace(timeslot_m.group(0), "")

            # find (lab), (lec)
            if class_type_m := re.search(r"\(?(lab|lec|лек|сем)\)?", string, flags=re.IGNORECASE):
                self.class_type = class_type_m.group(1).lower()
                string = string.replace(class_type_m.group(0), "")

            # find (G1)
            if group_m := re.search(r"\(?(G\d+)\)?", string):
                self.group = group_m.group(1)
                string = string.replace(group_m.group(0), "")

            # find location (what is left)
            string = string.strip()
            if string:
                self.location = string

    occurrences: list[Occurrence] = Field(default_factory=list)
    """ List of occurrences of the electives """

    def __init__(self, **data: Any):
        super().__init__(**data)
        for line in self.original:
            self.occurrences.append(self.Occurrence(original=line))

    def generate_events(
        self, date: datetime.date, timeslot: tuple[datetime.time, datetime.time]
    ) -> Generator["ElectiveEvent", None, None]:
        """
        Generate events for the cell

        :param date: date of the events
        :param timeslot: timeslot of the events
        :return: generator of events
        """
        overall_start, overall_end = timeslot
        overall_start = datetime.datetime.combine(date, overall_start, tzinfo=MOSCOW_TZ)
        overall_end = datetime.datetime.combine(date, overall_end, tzinfo=MOSCOW_TZ)

        # iterate over occurrences
        for occurrence in self.occurrences:
            start = overall_start
            end = overall_end

            if occurrence.starts_at:
                start = datetime.datetime.combine(date, occurrence.starts_at, tzinfo=MOSCOW_TZ)

            if occurrence.ends_at:
                end = datetime.datetime.combine(date, occurrence.ends_at, tzinfo=MOSCOW_TZ)

            yield ElectiveEvent(
                elective=occurrence.elective,
                location=occurrence.location,
                class_type=occurrence.class_type,
                group=occurrence.group,
                start=start,
                end=end,
            )


class ElectiveEvent(BaseModel):
    """
    Elective event model
    """

    elective: Elective
    """ Elective object """
    start: datetime.datetime
    """ Event start time """
    end: datetime.datetime
    """ Event end time """
    location: str | None = None
    """ Event location """
    class_type: str | None = None
    """ Event type """
    group: str | None = None
    """ Group to which the event belongs """

    def __hash__(self):
        string_to_hash = str(
            (
                self.elective.alias,
                self.start.isoformat(),
                self.end.isoformat(),
                self.location,
                self.class_type,
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
            # "Location": self.location,
            "Subject": self.elective.name,
            "Instructor": self.elective.instructor,
            # "Type": self.class_type,
            "Group": self.group,
            "Time": f"{self.start.strftime('%H:%M')} - {self.end.strftime('%H:%M')}",
            "Date": self.start.strftime("%d.%m.%Y"),
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
        postfix = None

        if self.group is not None:
            postfix = f"{self.group}"

        if self.class_type is not None:
            postfix = f"{self.class_type}" if postfix is None else f"{postfix}, {self.class_type}"

        if postfix is not None:
            vevent["summary"] = f"{self.elective.name} ({postfix})"
        else:
            vevent["summary"] = self.elective.name

        vevent["dtstart"] = icalendar.vDatetime(self.start)
        vevent["dtend"] = icalendar.vDatetime(self.end)
        vevent["uid"] = self.get_uid()
        vevent["categories"] = self.elective.name
        vevent["description"] = self.description

        if self.location is not None:
            vevent["location"] = self.location

        if self.elective.name is not None:
            vevent["color"] = get_color(self.elective.name)

        return vevent
