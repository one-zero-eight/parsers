import datetime
from zlib import crc32

from pydantic import BaseModel
import icalendar

from src.utils import get_color


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
        if self.description is not None:
            vevent["description"] = self.description
        if self.location:
            vevent["location"] = self.location
        vevent["color"] = get_color(self.summary)
        return vevent
