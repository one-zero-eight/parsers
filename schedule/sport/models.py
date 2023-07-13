import datetime
from typing import Optional

from pydantic import BaseModel


class ResponseSports(BaseModel):
    class Sport(BaseModel):
        id: int
        name: str
        special: bool

    sports: list[Sport]


class ResponseSportSchedule(BaseModel):
    class SportScheduleEvent(BaseModel):
        class ExtendedProps(BaseModel):
            group_id: int
            training_class: str
            current_load: int
            capacity: int

        title: Optional[str]
        daysOfWeek: list[int]
        startTime: str
        endTime: str
        extendedProps: ExtendedProps

    __root__: list[SportScheduleEvent]


class Model(BaseModel):
    class ScheduleItem(BaseModel):
        weekday: int
        start: str
        end: str
        training_class: int

    class Trainer(BaseModel):
        trainer_first_name: str
        trainer_last_name: str
        trainer_email: str

    group_id: int
    group_name: Optional[str]
    capacity: Optional[int]
    # current_load: int
    trainer_first_name: Optional[str]
    trainer_last_name: Optional[str]
    trainer_email: Optional[str]
    trainers: list[Trainer]
    # is_enrolled: bool
    # can_enroll: bool
    schedule: list[ScheduleItem]


class SportScheduleEvent(BaseModel):
    sport: ResponseSports.Sport
    sport_schedule_event: ResponseSportSchedule.SportScheduleEvent
    start: datetime.datetime
    end: datetime.datetime
    location: Optional[str]  # SportScheduleEvent.training_class
    description: Optional[str]
