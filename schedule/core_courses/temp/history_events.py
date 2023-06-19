from core_courses.models import Subject, ScheduleEvent
import datetime

history = Subject(name="History")
instructor = "Andrey Vasilev"

history_events: list[ScheduleEvent] = [
    # {# 17/06: 14:20-15:50 #}
    ScheduleEvent(
        start_time=datetime.time(hour=14, minute=20),
        end_time=datetime.time(hour=15, minute=50),
        day=datetime.date(year=2023, month=6, day=17),
        location="Online",
    ),
    # {#    21/06: 19:30-21:00 #}
    ScheduleEvent(
        start_time=datetime.time(hour=19, minute=30),
        end_time=datetime.time(hour=21, minute=0),
        day=datetime.date(year=2023, month=6, day=21),
        location="Online",
    ),
    # {#    22/06: 19:30-21:00 #}
    ScheduleEvent(
        start_time=datetime.time(hour=19, minute=30),
        end_time=datetime.time(hour=21, minute=0),
        day=datetime.date(year=2023, month=6, day=22),
        location="Online",
    ),
    # {#    24/06: 19:30-21:00 #}
    ScheduleEvent(
        start_time=datetime.time(hour=19, minute=30),
        end_time=datetime.time(hour=21, minute=0),
        day=datetime.date(year=2023, month=6, day=24),
        location="Online",
    ),
    # {#    30/06: 16:00-17:30,
    ScheduleEvent(
        start_time=datetime.time(hour=16, minute=0),
        end_time=datetime.time(hour=17, minute=30),
        day=datetime.date(year=2023, month=6, day=30),
        location="Online",
    ),
    # {#    30/06: 17:40-19:10 OFFLINE #}
    ScheduleEvent(
        start_time=datetime.time(hour=17, minute=40),
        end_time=datetime.time(hour=19, minute=10),
        day=datetime.date(year=2023, month=6, day=30),
        location="Offline",
    ),
    # {#    01/07: 09:00-10:30,
    ScheduleEvent(
        start_time=datetime.time(hour=9, minute=0),
        end_time=datetime.time(hour=10, minute=30),
        day=datetime.date(year=2023, month=7, day=1),
        location="Online",
    ),
    # {#    01/07: 14:20-15:50 OFFLINE #}
    ScheduleEvent(
        start_time=datetime.time(hour=14, minute=20),
        end_time=datetime.time(hour=15, minute=50),
        day=datetime.date(year=2023, month=7, day=1),
        location="Offline",
    ),
    # {#    4/07: 10:40-12:10 OFFLINE #}
    ScheduleEvent(
        start_time=datetime.time(hour=10, minute=40),
        end_time=datetime.time(hour=12, minute=10),
        day=datetime.date(year=2023, month=7, day=4),
        location="Offline",
    ),
    # {#    5/07: 10:40-12:10 #}
    ScheduleEvent(
        start_time=datetime.time(hour=10, minute=40),
        end_time=datetime.time(hour=12, minute=10),
        day=datetime.date(year=2023, month=7, day=5),
        location="Online",
    ),
    # {#    11/07: 09:00-10:30,
    ScheduleEvent(
        start_time=datetime.time(hour=9, minute=0),
        end_time=datetime.time(hour=10, minute=30),
        day=datetime.date(year=2023, month=7, day=11),
        location="Online",
    ),
    # {#    11/07: 10:40-12:10 #}
    ScheduleEvent(
        start_time=datetime.time(hour=10, minute=40),
        end_time=datetime.time(hour=12, minute=10),
        day=datetime.date(year=2023, month=7, day=11),
        location="Online",
    ),
    # {#    12/07: 09:00-10:30,
    ScheduleEvent(
        start_time=datetime.time(hour=9, minute=0),
        end_time=datetime.time(hour=10, minute=30),
        day=datetime.date(year=2023, month=7, day=12),
        location="Online",
    ),
    # {#    12/07: 10:40-12:10 #}
    ScheduleEvent(
        start_time=datetime.time(hour=10, minute=40),
        end_time=datetime.time(hour=12, minute=10),
        day=datetime.date(year=2023, month=7, day=12),
        location="Online",
    ),
]

for event in history_events:
    event.subject = history
    event.instructor = instructor
