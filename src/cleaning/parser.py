import datetime
from zlib import crc32

import icalendar
import pandas as pd
from pydantic import BaseModel, field_validator, model_validator

from src.core_courses.parser import parser_type

from ..utils import get_color, nearest_weekday
from .config import CleaningParserConfig
from .parse_cleaning_html import get_xlsx_file, parse


class CleaningParser:
    def __init__(self, config: CleaningParserConfig):
        self.config = config
    
    def get_cleaning_events(self) -> list["CleaningEvent"]:
        """
        Get cleaning events

        :return: cleaning events
        :rtype: list[CleaningEvent]
        """

        events = []

        xlsx_file = get_xlsx_file(self.config.cleaning_spreadsheet_id)
        dfs = pd.read_excel(xlsx_file, sheet_name=None, header=None)

        parsed = parse(dfs)
        events = []

        for location, dates in parsed.items():
            events.append(
                CleaningEvent(
                    summary="Cleaning",
                    location=location,
                    date=dates[0],
                    rdate=dates,
                )
            )

        return events

    def get_linen_change_schedule(self) -> list["LinenChangeEvent"]:
        """
        Get linen change schedule

        :return: linen change schedule
        :rtype: list[LinenChangeEvent]
        """

        events = []

        for linen_change_entry in self.config.linen_change_entries:
            events.append(
                LinenChangeEvent(
                    summary=linen_change_entry.name,
                    location=linen_change_entry.location,
                    rrule=linen_change_entry.rrule,
                    rdate=linen_change_entry.dates,
                    description=linen_change_entry.description,
                    date=self.config.start_date,
                )
            )

        return events


class CleaningEvent(BaseModel):
    summary: str
    location: str
    date: datetime.date
    rdate: list[datetime.date]

    @field_validator("rdate", mode="before")
    @classmethod
    def remove_repeat_dates(cls, v):
        return list(set(v))

    def get_uid(self) -> str:
        """
        Get unique id of the event
        """
        string_to_hash = str(("cleaning", self.summary, self.location, self.date.isoformat()))
        hash_ = crc32(string_to_hash.encode("utf-8"))

        return f"{abs(hash_):x}#cleaning@innohassle.ru"

    def get_vevent(self) -> icalendar.Event:
        """
        Get icalendar event

        :return: icalendar event
        :rtype: icalendar.Event
        """
        event = icalendar.Event()
        event.add("summary", self.summary)
        event.add("location", self.location)
        event.add("dtstart", icalendar.vDate(self.date))
        event.add("uid", self.get_uid())
        self.rdate = sorted(self.rdate)
        event.add("rdate", self.rdate, parameters={"value": "DATE"})
        color = get_color(self.location[0])
        event.add("color", color)
        return event


class LinenChangeEvent(BaseModel):
    summary: str
    date: datetime.date
    description: str
    location: str
    rrule: dict[str, str | int] | None = None
    rdate: list[datetime.date] | None = None

    @model_validator(mode="after")
    def validate_schedule_source(self):
        if self.rrule is None and self.rdate is None:
            raise ValueError("Either rrule or rdate must be set for linen change event")
        if self.rrule is not None and self.rdate is not None:
            raise ValueError("Only one of rrule or rdate can be set for linen change event")
        return self

    @field_validator("rdate", mode="before")
    @classmethod
    def remove_repeat_dates(cls, v):
        if v is None:
            return None
        return list(set(v))

    def get_uid(self) -> str:
        """
        Get unique id of the event
        """
        string_to_hash = str(("linen", self.summary, self.location))
        hash_ = crc32(string_to_hash.encode("utf-8"))
        return f"{abs(hash_):x}#linen@innohassle.ru"

    def get_vevent(self) -> icalendar.Event:
        """
        Get icalendar event
        """
        event = icalendar.Event()
        event.add("summary", self.summary)
        event.add("description", self.description)
        event.add("location", self.location)

        if self.rdate is not None:
            self.rdate = sorted(self.rdate)
            event.add("dtstart", icalendar.vDate(self.rdate[0]))
            event.add("rdate", self.rdate, parameters={"value": "DATE"})
        elif self.rrule is not None:
            event.add("dtstart", icalendar.vDate(nearest_weekday(self.date, self.rrule["byday"])))

            rrule = icalendar.vRecur(**self.rrule)
            event.add("rrule", rrule)
        event.add("uid", self.get_uid())
        color = get_color(self.location[0])
        event.add("color", color)

        return event
