from zlib import crc32

import icalendar

from ..utils import get_color
from .cell_to_event import ElectiveEvent


def get_event_hash(event: ElectiveEvent) -> int:
    """
    Get hash for the event

    :param event: The elective event
    :return: Hash value
    """
    string_to_hash = str(
        (
            event.elective.alias,
            event.start.isoformat(),
            event.end.isoformat(),
            event.location,
            event.class_type,
            event.group,
        )
    )
    return crc32(string_to_hash.encode("utf-8"))


def get_uid(event: ElectiveEvent) -> str:
    """
    Get unique identifier for the event

    :param event: The elective event
    :return: Unique identifier string
    """
    return f"{abs(get_event_hash(event)):x}@innohassle.ru"


def get_description(event: ElectiveEvent) -> str:
    """
    Get description for the event

    :param event: The elective event
    :return: Description string
    """
    r = {
        # "Location": event.location,
        "Subject": event.elective.name,
        "Instructor": event.elective.instructor,
        # "Type": event.class_type,
        "Group": event.group,
        "Time": f"{event.start.strftime('%H:%M')} - {event.end.strftime('%H:%M')}",
        "Date": event.start.strftime("%d.%m.%Y"),
    }

    r = {k: v for k, v in r.items() if v}
    return "\n".join([f"{k}: {v}" for k, v in r.items()])


def get_summary(event: ElectiveEvent) -> str:
    """
    Get summary for the event

    :param event: The elective event
    :return: Summary string
    """
    postfix = None

    if event.group is not None:
        postfix = f"{event.group}"

    if event.class_type is not None:
        postfix = f"{event.class_type}" if postfix is None else f"{postfix}, {event.class_type}"

    elective_name = event.elective.name or event.elective.alias
    if postfix is not None:
        return f"{elective_name} ({postfix})"
    else:
        return elective_name


def generate_vevent(event: ElectiveEvent, spreadsheet_id: str, gid: str) -> icalendar.Event:
    """
    Generate icalendar event from an ElectiveEvent

    :param event: The elective event to convert
    :param spreadsheet_id: Google Spreadsheet ID
    :param gid: Google Spreadsheet sheet GID
    :return: icalendar event
    """
    vevent = icalendar.Event()

    vevent["summary"] = get_summary(event)
    vevent["dtstart"] = icalendar.vDatetime(event.start)
    vevent["dtend"] = icalendar.vDatetime(event.end)
    vevent["uid"] = get_uid(event)
    if event.elective.name is not None:
        vevent["categories"] = event.elective.name
    vevent["description"] = get_description(event)
    if event.a1:
        vevent["x-wr-link"] = (
            f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}?gid={gid}#gid={gid}&range={event.a1}"
        )
    else:
        vevent["x-wr-link"] = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}?gid={gid}#gid={gid}"

    if event.location is not None:
        vevent["location"] = event.location

    if event.elective.name is not None:
        vevent["color"] = get_color(event.elective.name)

    return vevent
