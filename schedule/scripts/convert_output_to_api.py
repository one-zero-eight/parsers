import json

from pydantic import BaseModel

from schedule.utils import get_project_root


class InJsonCalendar(BaseModel):
    path: str
    name: str
    type: str
    satellite: dict[str, str]


class OutputData(BaseModel):
    groups: list[InJsonCalendar]


if __name__ == "__main__":
    output_directory = get_project_root() / "output"
    sports_json = json.loads((output_directory / "sports.json").read_text())
    core_courses_json = json.loads((output_directory / "core-courses.json").read_text())
    elective_courses_json = json.loads(
        (output_directory / "electives.json").read_text()
    )

    groups = []

    for calendar_dict in (sports_json, core_courses_json, elective_courses_json):
        for calendar in calendar_dict["calendars"]:
            groups.append(InJsonCalendar(**calendar))

    with open(output_directory / "predefined_groups.json", "w") as file:
        json_data = OutputData(groups=groups).json(
            indent=4,
        )
        file.write(json_data)
