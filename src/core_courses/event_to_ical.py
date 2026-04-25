import datetime
import warnings
from collections.abc import Generator
from datetime import UTC
from zlib import crc32

import icalendar

from src.logging_ import logger

from ..utils import MOSCOW_TZ, get_color, nearest_weekday
from .cell_to_event import CoreCourseEvent
from .location_parser import Item


def every_week_rule(event: CoreCourseEvent, *, ends: datetime.date | None = None) -> icalendar.vRecur:
    """
    Set recurrence rule and recurrence date for event
    """
    until = datetime.datetime.combine(ends or event.ends, datetime.time.min).astimezone(UTC)
    rrule = icalendar.vRecur({"WKST": "MO", "FREQ": "WEEKLY", "INTERVAL": 1, "UNTIL": until})
    return rrule


def get_event_hash(event: CoreCourseEvent) -> int:
    string_to_hash = str(
        (
            "core courses",
            event.course,
            event.start_time.isoformat(),
            event.end_time.isoformat(),
            event.starts.isoformat(),
            event.ends.isoformat(),
            event.weekday,
            *event.original_value,
        )
    )
    return crc32(string_to_hash.encode("utf-8"))


def get_uid(event: CoreCourseEvent, sequence: str = "x") -> str:
    """
    Get unique identifier for the event

    :return: unique identifier
    :rtype: str
    """
    return sequence + f"-{abs(get_event_hash(event)):x}@innohassle.ru"


def get_summary(event: CoreCourseEvent) -> str:
    return f"{event.subject} ({event.class_type})" if event.class_type else event.subject


def get_description(event: CoreCourseEvent) -> str | None:
    r = {
        # "Location": event.location,
        "Instructor": event.teacher,
        # "Type": event.class_type,
        "Group": event.group,
        "Course": event.course,
        # "Subject": event.subject,
    }

    r = {k: v for k, v in r.items() if v}
    if r:
        return "\n".join([f"{k}: {v}" for k, v in r.items()])
    return None


def generate_vevents(event: CoreCourseEvent) -> Generator[icalendar.Event, None, None]:
    """
    Generate icalendar events from a CoreCourseEvent

    :param event: The core course event to convert
    :return: icalendar events if "only on xx/xx, xx/xx" appeared in location, else one icalendar event
    """

    xwr_link = f"https://docs.google.com/spreadsheets/d/{event.spreadsheet_id}?gid={event.google_sheet_gid}#gid={event.google_sheet_gid}&range={event.a1}"
    if not event.location_item:
        start_of_weekdays = nearest_weekday(event.starts, event.weekday)
        dtstart = datetime.datetime.combine(start_of_weekdays, event.start_time, tzinfo=MOSCOW_TZ)
        dtend = datetime.datetime.combine(start_of_weekdays, event.end_time, tzinfo=MOSCOW_TZ)
        mapping = {
            "summary": get_summary(event),
            "description": get_description(event),
            "location": event.location,
            "dtstamp": icalendar.vDatetime(event.dtstamp),
            "uid": get_uid(event),
            "color": get_color(event.subject),
            "rrule": every_week_rule(event),
            "dtstart": icalendar.vDatetime(dtstart),
            "dtend": icalendar.vDatetime(dtend),
            "x-wr-link": xwr_link,
        }
        vevent = icalendar.Event()
        for key, value in mapping.items():
            if value:
                vevent.add(key, value)
        yield vevent
        return

    def clamp_ends(ends_on: datetime.date | None, fallback: datetime.date) -> datetime.date:
        if ends_on is None:
            return fallback
        return min(ends_on, fallback)

    def clamp_starts(starts_from: datetime.date | None, fallback: datetime.date) -> datetime.date:
        if starts_from is None:
            return fallback
        return max(starts_from, fallback)

    def convert_weeks_on_to_only_on(item: Item):
        if item.on_weeks:
            on = []
            for week in item.on_weeks:
                on_date = nearest_weekday(event.starts, event.weekday) + datetime.timedelta(weeks=week - 1)
                on.append(on_date)
            if item.on:
                item.on.extend(on)
            elif on:
                item.on = on
        if item.on:
            item.on = sorted(set(item.on))

    location_item = event.location_item
    location = location_item.location or event.location
    starts = clamp_starts(location_item.starts_from, event.starts)
    ends = clamp_ends(location_item.ends_on, event.ends)
    start_time = event.start_time
    end_time = event.end_time
    # move event start time to location starts_at time keeping same duration
    if location_item.starts_at:
        _start_time = datetime.datetime.combine(starts, start_time, tzinfo=MOSCOW_TZ)
        _end_time = datetime.datetime.combine(starts, end_time, tzinfo=MOSCOW_TZ)
        duration = _end_time - _start_time
        start_time = location_item.starts_at
        end_time = (datetime.datetime.combine(starts, start_time, tzinfo=MOSCOW_TZ) + duration).time()
    if location_item.till:
        end_time = location_item.till

    convert_weeks_on_to_only_on(location_item)
    start_of_weekdays = nearest_weekday(starts, event.weekday)
    if start_of_weekdays > ends:
        logger.warning(f"Event {event} has no occurrences after applying location date bounds")
        return
    dtstart = datetime.datetime.combine(start_of_weekdays, start_time, tzinfo=MOSCOW_TZ)
    dtend = datetime.datetime.combine(start_of_weekdays, end_time, tzinfo=MOSCOW_TZ)
    duration = dtend - dtstart

    mapping = {
        "summary": get_summary(event),
        "description": get_description(event),
        "location": location,
        "dtstamp": icalendar.vDatetime(event.dtstamp),
        "uid": get_uid(event),
        "color": get_color(event.subject),
        "x-wr-link": xwr_link,
    }

    vevent = icalendar.Event()

    for key, value in mapping.items():
        if value:
            vevent.add(key, value)

    if location_item.on:  # only on specific dates, not every week
        rdates = [dtstart.replace(day=on.day, month=on.month) for on in location_item.on if starts <= on <= ends]
        if not rdates:
            logger.warning(f"Event {event} has no rdates")
            return
        vevent.add("rdate", rdates)
        # dtstart and dtend should be adapted
        dtstart = rdates[0]
        dtend = dtend.replace(day=dtstart.day, month=dtstart.month)
    else:  # every week at the same time
        vevent.add("rrule", every_week_rule(event, ends=ends))

    vevent["dtstart"] = icalendar.vDatetime(dtstart)
    vevent["dtend"] = icalendar.vDatetime(dtend)

    # check for item.except_ and add exdate if needed
    if location_item.except_:
        exdates = [dtstart.replace(day=on.day, month=on.month) for on in location_item.except_]
        vevent.add("exdate", exdates)

    nested_on = []
    extra_nested = []
    if location_item.NEST:
        for item in location_item.NEST:
            convert_weeks_on_to_only_on(item)
            if item.on:
                nested_on.append(item)
            else:
                logger.warning(f"Root Item: {location_item}, {event.original_value}")
                extra_nested.append(item)

    if not (nested_on or extra_nested):  # Simple case, only one event
        yield vevent
        return

    if extra_nested:  # TODO: Handle '421 (316 FROM 31/10)' case
        warnings.warn(f"Extra nested is not implemented yet\nItem({location_item})")

    # NEST
    if vevent.has_key("rrule"):  # event with rrule
        seq = 0

        for i, item in enumerate(nested_on):
            # override specific recurrence entry
            item_starts = clamp_starts(item.starts_from, starts)
            item_ends = clamp_ends(item.ends_on, ends)
            for j, on in enumerate(item.on):
                if item_starts > on or on > item_ends:
                    continue
                seq += 1
                vevent_copy = vevent.copy()
                _recurrence_id = dtstart.replace(day=on.day, month=on.month)
                vevent_copy["recurrence-id"] = icalendar.vDatetime(_recurrence_id)
                vevent_copy["sequence"] = seq
                vevent_copy.pop("rrule")
                # adapt dtstart and dtend
                _dtstart = dtstart.replace(day=on.day, month=on.month)
                _dtend = dtend.replace(day=on.day, month=on.month)
                vevent_copy["dtstart"] = icalendar.vDatetime(_dtstart)
                vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
                if item.location:
                    vevent_copy["location"] = item.location
                if item.starts_at:
                    _dtstart = _dtstart.replace(hour=item.starts_at.hour, minute=item.starts_at.minute)
                    _dtend = _dtstart + duration
                    vevent_copy["dtstart"] = icalendar.vDatetime(_dtstart)
                    vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
                if item.till:
                    _dtend = _dtend.replace(hour=item.till.hour, minute=item.till.minute)
                    vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
                yield vevent_copy
        yield vevent
    else:  # just a single event on specific dates
        yield vevent

        for i, item in enumerate(nested_on):
            vevent_copy = vevent.copy()
            vevent_copy["uid"] = get_uid(event, sequence=str(i))
            vevent_copy.pop("rdate")
            item_starts = clamp_starts(item.starts_from, starts)
            item_ends = clamp_ends(item.ends_on, ends)
            rdates = [dtstart.replace(day=on.day, month=on.month) for on in item.on if item_starts <= on <= item_ends]
            if not rdates:
                continue
            vevent_copy.add("rdate", rdates)
            # adapt dtstart and dtend
            _dtstart = rdates[0]
            _dtend = dtend.replace(day=_dtstart.day, month=_dtstart.month)
            vevent_copy["dtstart"] = icalendar.vDatetime(_dtstart)
            vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
            if item.location:
                vevent_copy["location"] = item.location
            if item.starts_at:
                _dtstart = _dtstart.replace(hour=item.starts_at.hour, minute=item.starts_at.minute)
                _dtend = _dtstart + duration
                vevent_copy["dtstart"] = icalendar.vDatetime(_dtstart)
                vevent_copy["dtend"] = icalendar.vDatetime(_dtend)
            if item.till:
                _dtend = _dtend.replace(hour=item.till.hour, minute=item.till.minute)
                vevent_copy["dtend"] = icalendar.vDatetime(_dtend)

            yield vevent_copy
