import datetime
import re
from typing import Optional, Any, Generator
from zlib import crc32

import icalendar
from pydantic import BaseModel, validator, Field

from schedule.config_base import CSS3Color
from schedule.processors.regex import symbol_translation, process_spaces


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
    def beatify_string(cls: type["Elective"], string: str) -> str:
        """Beatify string

        :param string: string to beatify
        :type string: str
        :return: beatified string
        :rtype: str
        """
        if string:
            string = process_spaces(string)
            string = string.translate(symbol_translation)
        return string

    @property
    def color(self: "Elective") -> CSS3Color:
        """
        Get color for the subject

        :return: color for the subject
        """

        color_count = len(CSS3Color)
        hash_ = crc32(self.name.encode("utf-8"))
        return CSS3Color.get_by_index(hash_ % color_count)


class ElectiveCell(BaseModel):
    original: list[str]
    """ Original cell value """

    class Occurrence(BaseModel):
        """Occurrence of the elective"""

        original: str
        """ Original occurrence value """
        elective: Optional[Elective]
        """ Elective object """
        location: Optional[str]
        """ Location of the elective """
        group: Optional[str]
        """ Group to which the elective belongs """
        class_type: Optional[str]
        """ Type of the class(leture, seminar, etc.) """
        starts_at: Optional[datetime.time]
        """ Time when the elective starts (modificator) """
        ends_at: Optional[datetime.time]
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

            from schedule.electives.config import electives_config as config

            super().__init__(**data)

            string = self.original.strip()

            # just first word as elective
            splitter = string.split(" ")
            elective_alias = splitter[0]
            self.elective = next(
                elective
                for elective in config.ELECTIVES
                if elective.alias == elective_alias
            )
            string = " ".join(splitter[1:])
            # find time xx:xx-xx:xx

            if timeslot_m := re.search(r"\(?(\d{2}:\d{2})-(\d{2}:\d{2})\)?", string):
                self.starts_at = datetime.datetime.strptime(
                    timeslot_m.group(1), "%H:%M"
                ).time()
                self.ends_at = datetime.datetime.strptime(
                    timeslot_m.group(2), "%H:%M"
                ).time()
                string = string.replace(timeslot_m.group(0), "")

            # find starts at xx:xx
            if timeslot_m := re.search(r"\(?starts at (\d{2}:\d{2})\)?", string):
                self.starts_at = datetime.datetime.strptime(
                    timeslot_m.group(1), "%H:%M"
                ).time()
                string = string.replace(timeslot_m.group(0), "")

            # find (lab), (lec)
            if class_type_m := re.search(
                r"\(?(lab|lec)\)?", string, flags=re.IGNORECASE
            ):
                self.class_type = class_type_m.group(1).lower()
                string = string.replace(class_type_m.group(0), "")

            # find (Group 1)
            if group_m := re.search(r"\(?(Group \d+)\)?", string):
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
        overall_start = datetime.datetime.combine(date, overall_start)
        overall_end = datetime.datetime.combine(date, overall_end)

        # iterate over occurrences
        for occurrence in self.occurrences:
            start = overall_start
            end = overall_end

            if occurrence.starts_at:
                start = datetime.datetime.combine(date, occurrence.starts_at)

            if occurrence.ends_at:
                end = datetime.datetime.combine(date, occurrence.ends_at)

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
    location: Optional[str]
    """ Event location """
    class_type: Optional[str]
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
        vevent["summary"] = self.elective.name
        if self.class_type is not None:
            vevent["summary"] += f" ({self.class_type})"
        vevent["dtstart"] = icalendar.vDatetime(self.start)
        vevent["dtend"] = icalendar.vDatetime(self.end)
        vevent["uid"] = self.get_uid()
        vevent["categories"] = self.elective.name
        vevent["description"] = self.description

        if self.location is not None:
            vevent["location"] = self.location

        if hasattr(self.elective, "color"):
            vevent["color"] = self.elective.color

        return vevent
