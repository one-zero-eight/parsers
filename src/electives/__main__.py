import asyncio
import json
import os
from pathlib import Path

from src.config_base import SaveConfig, from_yaml
from src.electives.config import ElectivesParserConfig
from src.electives.event_to_ical import generate_vevent
from src.electives.parser import ElectiveParser
from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.utils import fetch_xlsx_spreadsheet, get_base_calendar, get_sheet_gids, sanitize_sheet_name, sluggify


async def main():
    config_path = Path(__file__).parent / "config.yaml"
    parser_config = from_yaml(ElectivesParserConfig, config_path)
    save_config = from_yaml(SaveConfig, config_path)
    if not parser_config.spreadsheet_id:
        logger.error("Spreadsheet ID is not set")
        return None

    parser = ElectiveParser()
    xlsx = await fetch_xlsx_spreadsheet(spreadsheet_id=parser_config.spreadsheet_id)
    original_target_sheet_names = [target.sheet_name for target in parser_config.targets]
    pipeline_result = parser.pipeline(xlsx, original_target_sheet_names, parser_config.electives)

    # Get sheet name -> gid mapping
    logger.info("Fetching sheet gids from Google Spreadsheet...")
    sheet_gids = await get_sheet_gids(parser_config.spreadsheet_id)
    logger.debug(f"Found sheet gids: {sheet_gids}")

    # -------- Convert to Icalendar --------
    semester_tag = CreateTag(
        alias=parser_config.semester_tag.alias,
        type=parser_config.semester_tag.type,
        name=parser_config.semester_tag.name,
    )
    elective_tag = CreateTag(
        alias="electives",
        type="category",
        name="Electives",
    )
    tags = [semester_tag, elective_tag]
    predefined_event_groups: list[CreateEventGroup] = []
    mount_point = save_config.save_ics_path

    for target, separations in zip(parser_config.targets, pipeline_result):
        elective_type_directory = mount_point / sluggify(target.sheet_name)
        elective_type_directory.mkdir(parents=True, exist_ok=True)
        elective_type_tag = CreateTag(alias=sluggify(target.sheet_name), type="electives", name=target.sheet_name)

        tags.append(elective_type_tag)

        for elective_separation in separations:
            calendar = get_base_calendar()
            elective = elective_separation.elective
            calendar["x-wr-calname"] = elective.alias
            calendar["x-wr-link"] = f"https://docs.google.com/spreadsheets/d/{parser_config.spreadsheet_id}"

            cnt = 0

            for event in elective_separation.events:
                # Get gid for this event's sheet
                gid = None
                if event.sheet_name:
                    # Try exact match first
                    gid = sheet_gids.get(event.sheet_name)
                    # If not found, try sanitized match
                    if gid is None:
                        sanitized_name = sanitize_sheet_name(event.sheet_name)
                        for sheet_name, sheet_gid in sheet_gids.items():
                            if sanitize_sheet_name(sheet_name) == sanitized_name:
                                gid = sheet_gid
                                break
                    if gid is None:
                        logger.warning(f"Could not find gid for sheet '{event.sheet_name}', using first available gid")
                        gid = next(iter(sheet_gids.values())) if sheet_gids else "0"
                else:
                    logger.warning("Event has no sheet_name, using first available gid")
                    gid = next(iter(sheet_gids.values())) if sheet_gids else "0"

                calendar.add_component(generate_vevent(event, parser_config.spreadsheet_id, gid))
                cnt += 1

            calendar.add("x-wr-total-vevents", str(cnt))

            elective_x_group_alias = sluggify(elective.alias)
            calendar_alias = (
                f"{parser_config.semester_tag.alias}-{sluggify(target.sheet_name)}-{elective_x_group_alias}"
            )

            file_name = f"{elective_x_group_alias}.ics"
            file_path = elective_type_directory / file_name

            logger.info(f"> Writing {file_path}")

            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, "wb") as f:
                content = calendar.to_ical()
                # TODO: add validation
                f.write(content)

            description = f"Elective schedule for '{elective.name or elective.alias}'"
            predefined_event_groups.append(
                CreateEventGroup(
                    alias=calendar_alias,
                    name=elective.name or elective.alias,
                    description=description,
                    path=file_path.relative_to(save_config.mount_point).as_posix(),
                    tags=[
                        elective_tag,
                        elective_type_tag,
                        semester_tag,
                    ],
                )
            )

    logger.info(f"Writing JSON file... {len(predefined_event_groups)} event groups.")
    output = Output(event_groups=predefined_event_groups, tags=tags)
    # create a new .json file with information about calendar
    with open(save_config.save_json_path, "w") as f:
        json.dump(output.model_dump(), f, indent=2, sort_keys=False)
    # InNoHassle integration
    if save_config.innohassle_api_url is None or save_config.parser_auth_key is None:
        logger.info("Skipping InNoHassle integration")
        return
    inh_client = InNoHassleEventsClient(
        api_url=save_config.innohassle_api_url,
        parser_auth_key=save_config.parser_auth_key.get_secret_value(),
    )
    result = await update_inh_event_groups(inh_client, save_config.mount_point, output)
    return result


if __name__ == "__main__":
    asyncio.run(main())
