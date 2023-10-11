import datetime
import logging
from zlib import crc32

import icalendar
from pydantic import BaseModel, validator
from schedule.config_base import CSS3Color

from schedule.cleaning.config import CleaningParserConfig


class CleaningParser:
    logger = logging.getLogger(__name__ + "." + "Parser")

    config: CleaningParserConfig

    def __init__(self, config: CleaningParserConfig):
        self.config = config

    def get_cleaning_events(self) -> list["CleaningEvent"]:
        """
        Get cleaning events

        :return: cleaning events
        :rtype: list[CleaningEvent]
        """
        events = []

        for cleaning_entry in self.config.CLEANING_ENTRIES:
            self.logger.info(f"Processing {cleaning_entry.name}")
            events.append(
                CleaningEvent(
                    summary=cleaning_entry.name,
                    location=cleaning_entry.location,
                    date=cleaning_entry.dates[0],
                    rdate=cleaning_entry.dates,
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

        for linen_change_entry in self.config.LINEN_CHANGE_ENTRIES:
            self.logger.info(f"Processing {linen_change_entry.name}")
            events.append(
                LinenChangeEvent(
                    summary=linen_change_entry.name,
                    location=linen_change_entry.location,
                    rrule=linen_change_entry.rrule,
                    date=self.config.START_DATE,
                )
            )

        return events


class CleaningEvent(BaseModel):
    summary: str
    location: str
    date: datetime.date
    rdate: list[datetime.date]

    @validator("rdate")
    def remove_repeat_dates(cls, v):
        return list(set(v))

    def __hash__(self) -> int:
        """
        Hash of the event

        :return: hash of the event
        :rtype: int
        """
        string_to_hash = str(
            (
                "cleaning",
                self.summary,
                self.location,
                self.date.isoformat(),
            )
        )
        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self) -> str:
        """
        Get unique id of the event

        :return: unique id of the event
        :rtype: str
        """
        return "%x#cleaning@innohassle.ru" % abs(hash(self))

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
        hash_ = crc32(self.location[0].encode("utf-8"))
        color = CSS3Color.get_by_index(hash_ % len(CSS3Color))
        event.add("color", color)
        return event


class LinenChangeEvent(BaseModel):
    summary: str
    date: datetime.date
    description: str = "Working hours:\n10:00-12:00\n13:00-17:00"
    location: str
    rrule: dict

    def __hash__(self) -> int:
        """
        Hash of the event

        :return: hash of the event
        :rtype: int
        """
        string_to_hash = str(
            (
                "linen",
                self.summary,
                self.location,
            )
        )
        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self) -> str:
        """
        Get unique id of the event

        :return: unique id of the event
        :rtype: str
        """
        return "%x#linen@innohassle.ru" % abs(hash(self))

    def get_vevent(self) -> icalendar.Event:
        """
        Get icalendar event

        :return: icalendar event
        :rtype: icalendar.Event
        """
        event = icalendar.Event()
        event.add("summary", self.summary)
        event.add("description", self.description)
        event.add("location", self.location)
        from schedule.utils import nearest_weekday

        event.add(
            "dtstart", icalendar.vDate(nearest_weekday(self.date, self.rrule["byday"]))
        )

        rrule = icalendar.vRecur(
            freq=self.rrule["freq"],
            byday=self.rrule["byday"],
            until=datetime.datetime.strptime(self.rrule["until"], "%Y-%m-%d"),
        )
        event.add("rrule", rrule)
        event.add("uid", self.get_uid())
        hash_ = crc32(self.location[0].encode("utf-8"))
        color = CSS3Color.get_by_index(hash_ % len(CSS3Color))
        event.add("color", color)

        return event
