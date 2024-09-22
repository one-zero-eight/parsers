import asyncio
import json
from hashlib import sha1

import pandas as pd

from src.electives.config import electives_config as config
from src.electives.parser import ElectiveParser, convert_separation
from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.processors.regex import sluggify
from src.utils import get_base_calendar


def main():
    parser = ElectiveParser()
    xlsx = parser.get_xlsx_file(
        spreadsheet_id=config.SPREADSHEET_ID,
    )
    dfs = parser.get_clear_dataframes_from_xlsx(
        xlsx_file=xlsx,
        targets=config.TARGETS,
    )
    # noinspection InsecureHash
    to_hash = (sha1(pd.util.hash_pandas_object(dfs[target.sheet_name]).values).hexdigest() for target in config.TARGETS)
    # noinspection InsecureHash
    hashsum = sha1("\n".join(to_hash).encode("utf-8")).hexdigest()
    logger.info(f"Hashsum: {hashsum}")
    xlsx_path = config.TEMP_DIR / f"{hashsum}.xlsx"
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    # check if file exists
    if xlsx_path.exists():
        logger.info("Hashsum match!")
    with open(xlsx_path, "wb") as f:
        logger.info(f"Loading cached file {hashsum}.xlsx")
        xlsx.seek(0)
        content = xlsx.read()
        f.write(content)
    semester_tag = CreateTag(
        alias=config.SEMESTER_TAG.alias,
        type=config.SEMESTER_TAG.type,
        name=config.SEMESTER_TAG.name,
    )
    elective_tag = CreateTag(
        alias="electives",
        type="category",
        name="Electives",
    )
    tags = [semester_tag, elective_tag]
    predefined_event_groups: list[CreateEventGroup] = []
    mount_point = config.SAVE_ICS_PATH
    for target in config.TARGETS:
        logger.info(f"Processing {target.sheet_name}... Range: {target.range}")

        sheet_df = next(df for sheet_name, df in dfs.items() if sheet_name.startswith(target.sheet_name))
        by_weeks = parser.split_df_by_weeks(sheet_df)
        index = {}
        for sheet_df in by_weeks:
            index.update(sheet_df.index)
        big_df = pd.DataFrame(index=list(index))
        big_df = pd.concat([big_df, *by_weeks], axis=1)
        big_df.dropna(axis=1, how="all", inplace=True)
        big_df.dropna(axis=0, how="all", inplace=True)
        events = list(parser.parse_df(big_df))

        converted = convert_separation(events)

        elective_type_directory = mount_point / sluggify(target.sheet_name)

        elective_type_directory.mkdir(parents=True, exist_ok=True)

        elective_type_tag = CreateTag(
            alias=sluggify(target.sheet_name),
            type="electives",
            name=target.sheet_name,
        )

        tags.append(elective_type_tag)

        for calendar_name, events in converted.items():
            calendar = get_base_calendar()
            calendar["x-wr-calname"] = calendar_name
            calendar["x-wr-link"] = f"https://docs.google.com/spreadsheets/d/{config.SPREADSHEET_ID}"

            cnt = 0

            for event in events:
                calendar.add_component(event.get_vevent())
                cnt += 1

            calendar.add("x-wr-total-vevents", str(cnt))

            elective_x_group_alias = sluggify(calendar_name)
            calendar_alias = f"{config.SEMESTER_TAG.alias}-{sluggify(target.sheet_name)}-{elective_x_group_alias}"

            file_name = f"{elective_x_group_alias}.ics"
            file_path = elective_type_directory / file_name

            logger.info(f"> Writing {file_path.relative_to(config.MOUNT_POINT)}")

            with open(file_path, "wb") as f:
                content = calendar.to_ical()
                # TODO: add validation
                f.write(content)

            calendar_name = calendar_name.replace("-", " ")

            if events:
                description = events[0].elective.name
            else:
                description = f"Elective schedule for '{calendar_name}'"
            predefined_event_groups.append(
                CreateEventGroup(
                    alias=calendar_alias,
                    name=calendar_name,
                    description=description,
                    path=file_path.relative_to(config.MOUNT_POINT).as_posix(),
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
    with open(config.SAVE_JSON_PATH, "w") as f:
        json.dump(output.dict(), f, indent=2, sort_keys=False)
    # InNoHassle integration
    if config.INNOHASSLE_API_URL is None or config.PARSER_AUTH_KEY is None:
        logger.info("Skipping InNoHassle integration")
        return
    inh_client = InNoHassleEventsClient(
        api_url=config.INNOHASSLE_API_URL,
        parser_auth_key=config.PARSER_AUTH_KEY.get_secret_value(),
    )
    result = asyncio.run(update_inh_event_groups(inh_client, config.MOUNT_POINT, output))
    return result

if __name__ == "__main__":
    main()
