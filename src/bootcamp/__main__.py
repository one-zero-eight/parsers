import json
from itertools import groupby

from src.bootcamp.config import bootcamp_config as config
from src.bootcamp.models import BootcampEvent
from src.bootcamp.parser import BootcampParser
from src.utils import get_base_calendar

if __name__ == "__main__":
    parser = BootcampParser()
    events = parser.get_events()
    common_events = list(filter(lambda e: e.group is None, events))
    specific_events = list(filter(lambda e: e.group is not None, events))
    specific_events.sort(key=lambda e: e.group)
    directory = config.SAVE_ICS_PATH
    json_file = config.SAVE_JSON_PATH
    bootcamp_alias = f"bootcamp{config.YEAR_OF_BOOTCAMP}"
    bootcamp_tag = {"alias": bootcamp_alias, "type": "category"}
    academician_tag = {"alias": "academic", "type": bootcamp_alias}
    json_data = {"calendars": [], "tags": [bootcamp_tag, academician_tag]}
    year_path = directory
    for group_name, grouper in groupby(specific_events, key=lambda e: e.group):
        group_events = list(grouper) + common_events
        group_calendar = get_base_calendar()
        group_calendar["x-wr-calname"] = f"Bootcamp 2023 {group_name}"

        for group_event in group_events:
            group_event: BootcampEvent
            group_calendar.add_component(group_event.get_vevent())
        group_slug = group_name.lower().replace(" ", "")
        file_name = f"{group_slug}.ics"
        file_path = year_path / file_name
        json_data["calendars"].append(
            {
                "path": file_path.relative_to(file_path.parents[-2]).as_posix(),
                "name": f"Bootcamp ({group_name})",
                "tags": [bootcamp_tag, academician_tag],
                "alias": f"{bootcamp_alias}-{group_slug}",
            }
        )

        with open(file_path, "wb") as f:
            f.write(group_calendar.to_ical())
        # create a new .json file with information about calendars
    with open(json_file, "w") as f:
        json.dump(json_data, f, indent=4, sort_keys=True)
