import datetime
from zlib import crc32

from pydantic import BaseModel
import icalendar
from src.config_base import CSS3Color


class Palitre:
    @staticmethod
    def get_by_summary(summary: str):
        h = crc32(summary.encode("utf-8")) % len(CSS3Color)
        return CSS3Color.get_by_index(h)


class WorkshopEvent(BaseModel):
    summary: str
    speaker: str = None
    comments: list[str] = None
    location: str = None
    capacity: str = None

    timeslots: list[tuple[datetime.time, datetime.time]] = None

    dtstart: datetime.datetime = None
    dtend: datetime.datetime = None

    def __hash__(self) -> int:
        string_to_hash = str(
            (
                "bootcamp",
                "workshops",
                self.summary,
                self.dtstart.isoformat(),
                self.dtend.isoformat(),
            )
        )

        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self) -> str:
        return "%x@innohassle.ru" % abs(hash(self))

    def set_datetime(self, start_time, end_time, date):
        self.dtstart = datetime.datetime.combine(date, start_time)
        self.dtend = datetime.datetime.combine(date, end_time)

    @property
    def description(self):
        description = ""
        if self.speaker:
            description += f"{self.speaker}\n"
        if self.comments:
            description += "\n".join(self.comments) + "\n"
        if self.capacity:
            description += f"Capacity: {self.capacity}\n"
        if self.location:
            description += f"Location: {self.location}\n"

        if len(self.timeslots) > 1:
            description += "Timeslots:\n"
            for i, timeslot in enumerate(self.timeslots):
                description += f"{i + 1}) {timeslot[0].strftime('%H:%M')} - {timeslot[1].strftime('%H:%M')}\n"
        else:
            description += (
                f"Timeslot: {self.timeslots[0][0].strftime('%H:%M')} - {self.timeslots[0][1].strftime('%H:%M')}\n"
            )

        return description

    def get_vevent(self):
        vevent = icalendar.Event(
            summary=self.summary,
            uid=self.get_uid(),
            categories=["bootcamp"],
            dtstart=icalendar.vDatetime(self.dtstart),
            dtend=icalendar.vDatetime(self.dtend),
        )
        description = self.description
        if description:
            vevent["description"] = description
        if self.location:
            vevent["location"] = self.location
        vevent["color"] = Palitre.get_by_summary(self.summary)
        return vevent
