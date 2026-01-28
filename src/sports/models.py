import datetime
from enum import StrEnum
from zlib import crc32

import icalendar
from pydantic import BaseModel, RootModel

from src.utils import MOSCOW_TZ, get_color, nearest_weekday


class ResponseSports(BaseModel):
    class Sport(BaseModel):
        id: int
        name: str
        special: bool

    sports: list[Sport]


class SportScheduleEventResponse(BaseModel):
    class ExtendedProps(BaseModel):
        group_id: int
        training_class: str | None = None
        current_load: int
        capacity: int

        def __hash__(self):
            string_to_hash = str(
                (
                    self.group_id,
                    self.training_class or "",
                )
            )

            return crc32(string_to_hash.encode("utf-8"))

    title: str | None
    daysOfWeek: list[int]
    startTime: str
    endTime: str
    extendedProps: ExtendedProps

    def __hash__(self):
        string_to_hash = str(
            (
                self.title,
                self.daysOfWeek,
                self.startTime,
                self.endTime,
                hash(self.extendedProps),
            )
        )

        return crc32(string_to_hash.encode("utf-8"))


class ResponseSportSchedule(RootModel[list[SportScheduleEventResponse]]):
    pass


class VDayOfWeek(StrEnum):
    MONDAY = "MO"
    TUESDAY = "TU"
    WEDNESDAY = "WE"
    THURSDAY = "TH"
    FRIDAY = "FR"
    SATURDAY = "SA"
    SUNDAY = "SU"

    @classmethod
    def get_by_index(cls, idx: int) -> "VDayOfWeek":
        return list(cls.__members__.values())[idx]


class SportScheduleEvent(BaseModel):
    sport: ResponseSports.Sport
    sport_schedule_event: SportScheduleEventResponse

    @property
    def summary(self) -> str:
        title = self.sport.name
        if subtitle := self.sport_schedule_event.title:
            title += " - " + subtitle
        return title

    @property
    def location(self) -> str | None:
        return self.sport_schedule_event.extendedProps.training_class

    @property
    def start(self) -> datetime.time:
        return datetime.datetime.strptime(self.sport_schedule_event.startTime, "%H:%M:%S").time()

    @property
    def end(self) -> datetime.time:
        return datetime.datetime.strptime(self.sport_schedule_event.endTime, "%H:%M:%S").time()

    def __hash__(self):
        string_to_hash = str(
            (
                self.sport.id,
                hash(self.sport_schedule_event),
            )
        )

        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self) -> str:
        """
        Get unique identifier for the event

        :return: unique identifier
        :rtype: str
        """
        return "%x@innohassle.ru" % abs(hash(self))

    @property
    def description(self) -> str:
        """
        Description of the event

        :return: description of the event
        :rtype: str
        """

        r = {
            "Location": self.location,
            "Time": f"{self.start.strftime('%H:%M')} - {self.end.strftime('%H:%M')}",
            "Special": self.sport.special if self.sport.special else None,
        }

        r = {k: v for k, v in r.items() if v}
        return "\n".join([f"{k}: {v}" for k, v in r.items()])

    def get_vevent(self, very_first_date: datetime.date, very_last_date: datetime.date) -> icalendar.Event:
        """
        Get icalendar event

        :return: icalendar event
        :rtype: icalendar.Event
        """
        vevent = icalendar.Event()
        vevent["summary"] = self.summary
        vevent["location"] = self.location
        vevent["uid"] = self.get_uid()
        starting = nearest_weekday(
            very_first_date,
            self.sport_schedule_event.daysOfWeek[0] - 1,  # 0 is Monday
        )
        dtstart = datetime.datetime.combine(starting, self.start, tzinfo=MOSCOW_TZ)
        dtend = datetime.datetime.combine(starting, self.end, tzinfo=MOSCOW_TZ)
        very_last_date_dt = datetime.datetime.combine(very_last_date, datetime.time.min, tzinfo=datetime.UTC)
        vevent["rrule"] = icalendar.vRecur(
            {
                "freq": "weekly",
                "until": very_last_date_dt,
                "byday": [
                    icalendar.vWeekday(VDayOfWeek.get_by_index(day - 1)) for day in self.sport_schedule_event.daysOfWeek
                ],
            }
        )

        vevent["dtstart"] = icalendar.vDatetime(dtstart)
        vevent["dtend"] = icalendar.vDatetime(dtend)
        vevent["categories"] = [
            icalendar.vText("sport"),
            icalendar.vText(self.sport.name),
        ]
        vevent["color"] = get_color(self.summary)
        vevent["description"] = icalendar.vText(self.description)

        return vevent
