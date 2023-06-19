import datetime
from typing import Optional
from zlib import crc32

import icalendar
from pydantic import BaseModel, validator

from processors.regex import symbol_translation, remove_trailing_spaces


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
    notes: Optional[str] = None
    """ Notes for the event """

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
            "Notes": self.notes,
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
        vevent["dtstart"] = icalendar.vDatetime(self.start)
        vevent["dtend"] = icalendar.vDatetime(self.end)
        vevent["uid"] = self.get_uid()
        vevent["categories"] = self.elective.name
        vevent["description"] = self.description

        if self.location is not None:
            vevent["location"] = self.location

        return vevent
