import asyncio
import json
from hashlib import sha1

import pandas as pd

from src.electives.config import electives_config as config
from src.electives.parser import ElectiveParser, convert_separation
from src.innohassle import CreateEventGroup, CreateTag, InNoHassleEventsClient, Output, update_inh_event_groups
from src.logging_ import logger
from src.utils import get_base_calendar, sluggify


def main():
    if not config.spreadsheet_id:
        logger.error("Spreadsheet ID is not set")
        return None

    parser = ElectiveParser()
    xlsx = parser.get_xlsx_file(
        spreadsheet_id=config.spreadsheet_id,
    )
    dfs = parser.get_clear_dataframes_from_xlsx(
        xlsx_file=xlsx,
        targets=config.targets,
    )
    # noinspection InsecureHash
    to_hash = (sha1(pd.util.hash_pandas_object(dfs[target.sheet_name]).values).hexdigest() for target in config.targets)
    # noinspection InsecureHash
    hashsum = sha1("\n".join(to_hash).encode("utf-8")).hexdigest()
    logger.info(f"Hashsum: {hashsum}")
    xlsx_path = config.temp_dir / f"{hashsum}.xlsx"
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
        alias=config.semester_tag.alias,
        type=config.semester_tag.type,
        name=config.semester_tag.name,
    )
    elective_tag = CreateTag(
        alias="electives",
        type="category",
        name="Electives",
    )
    tags = [semester_tag, elective_tag]
    predefined_event_groups: list[CreateEventGroup] = []
    mount_point = config.save_ics_path
    for target in config.targets:
        logger.info(f"Processing {target.sheet_name}... Range: {target.range}")

        sheet_df = next(df for sheet_name, df in dfs.items() if sheet_name == target.sheet_name)
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

        for elective_alias, (name, events) in converted.items():
            calendar = get_base_calendar()
            calendar["x-wr-calname"] = elective_alias
            calendar["x-wr-link"] = f"https://docs.google.com/spreadsheets/d/{config.spreadsheet_id}"

            cnt = 0

            for event in events:
                calendar.add_component(event.get_vevent())
                cnt += 1

            calendar.add("x-wr-total-vevents", str(cnt))

            elective_x_group_alias = sluggify(elective_alias)
            calendar_alias = f"{config.semester_tag.alias}-{sluggify(target.sheet_name)}-{elective_x_group_alias}"

            file_name = f"{elective_x_group_alias}.ics"
            file_path = elective_type_directory / file_name

            logger.info(f"> Writing {file_path.relative_to(config.mount_point)}")

            with open(file_path, "wb") as f:
                content = calendar.to_ical()
                # TODO: add validation
                f.write(content)

            elective_alias = elective_alias.replace("-", " ")

            if events:
                description = events[0].elective.name
            else:
                description = f"Elective schedule for '{elective_alias}'"
            predefined_event_groups.append(
                CreateEventGroup(
                    alias=calendar_alias,
                    name=name,
                    description=description,
                    path=file_path.relative_to(config.mount_point).as_posix(),
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
    with open(config.save_json_path, "w") as f:
        json.dump(output.dict(), f, indent=2, sort_keys=False)
    # InNoHassle integration
    if config.innohassle_api_url is None or config.parser_auth_key is None:
        logger.info("Skipping InNoHassle integration")
        return
    inh_client = InNoHassleEventsClient(
        api_url=config.innohassle_api_url,
        parser_auth_key=config.parser_auth_key.get_secret_value(),
    )
    result = asyncio.run(update_inh_event_groups(inh_client, config.mount_point, output))
    return result


if __name__ == "__main__":
    main()
