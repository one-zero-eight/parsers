import datetime
from zlib import crc32

from pydantic import BaseModel
import icalendar
from schedule.config_base import CSS3Color


class Palitre:
    @staticmethod
    def get_by_summary(summary: str):
        h = crc32(summary.encode("utf-8")) % len(CSS3Color)
        return CSS3Color.get_by_index(h)


class BootcampEvent(BaseModel):
    summary: str
    description: str = None
    location: str = None
    group: str = None

    dtstart: datetime.datetime = None
    dtend: datetime.datetime = None

    def __hash__(self) -> int:
        string_to_hash = str(
            (
                "bootcamp",
                self.summary,
                self.description,
                self.dtstart.isoformat(),
                self.dtend.isoformat(),
                self.group,
            )
        )

        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self) -> str:
        return "%x@innohassle.ru" % abs(hash(self))

    def set_datetime(self, start_time, end_time, date):
        self.dtstart = datetime.datetime.combine(date, start_time)
        self.dtend = datetime.datetime.combine(date, end_time)

    def get_vevent(self):
        vevent = icalendar.Event(
            summary=self.summary,
            uid=self.get_uid(),
            categories=["bootcamp"],
            dtstart=icalendar.vDatetime(self.dtstart),
            dtend=icalendar.vDatetime(self.dtend),
        )
        if self.description:
            vevent["description"] = self.description
        if self.location:
            vevent["location"] = self.location
        vevent["color"] = Palitre.get_by_summary(self.summary)
        return vevent
