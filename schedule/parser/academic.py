import json
import logging
import os.path
import re
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import pairwise
from pathlib import Path
from uuid import uuid4

import googleapiclient.discovery
import icalendar
import numpy as np
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from schedule.parser.config import academic_config as config, PARSER_PATH


class AcademicParser:
    spreadsheets: googleapiclient.discovery.Resource
    credentials: Credentials
    logger = logging.getLogger(__name__ + "." + "Parser")

    def __init__(
            self,
    ):

        self.credentials = self.init_api(
            Path(config.CREDENTIALS_PATH),
            scopes=config.API_SCOPES
        )
        self.spreadsheets = self.connect_spreadsheets()

    @staticmethod
    def init_api(credentials: Path, scopes: list[str]) -> Credentials:
        """
        Initialize API credentials.
        @param credentials: Path to credentials file.
        @type credentials: Path
        @param scopes: List of scopes to authorize.
        @type scopes: list[str]
        @return Current Credentials object.
        :rtype: Credentials
        """
        creds = None
        token_path = PARSER_PATH / "token.json"
        # The file token.json stores the user's access and refresh tokens, and
        # is created automatically when the authorization flow completes for
        # the first time.
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(
                token_path, scopes
            )

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(credentials), scopes
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_path, 'w') as token:
                token.write(creds.to_json())

        return creds

    def connect_spreadsheets(self):
        """
        Connect to Google Sheets API.
        @return service.spreadsheets()
        :rtype: googleapiclient.discovery.Resource
        """

        service = googleapiclient.discovery.build(
            'sheets',
            'v4',
            credentials=self.credentials
        )
        # Call the Sheets API
        return service.spreadsheets()

    def get_clear_df(
            self,
            spreadsheet_id: str,
            target_range: str,
            target_title: str
    ) -> pd.DataFrame:
        """
        Get data from Google Sheets and return it as a DataFrame with merged
        cells and empty cells in the course row filled by left value.

        Args:
            spreadsheet_id (str): ID of the spreadsheet to get data from.
            target_range (str): A1 notation of the values to retrieve. Or
             named range.
            target_title (str): Title of the sheet to retrieve data from.

        Returns:
            DataFrame with merged cells and empty cells in the course row
        """

        self.logger.debug("Getting dataframe from Google Sheets...")
        self.logger.info(
            f"Retrieving data: {spreadsheet_id}/{target_title}-{target_range}"
        )

        values = self.spreadsheets.values().get(
            spreadsheetId=spreadsheet_id,
            range=target_range
        ).execute()["values"]

        df = pd.DataFrame(data=values)
        # strip all values
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        max_x, max_y = df.shape

        self.logger.info(f"Data retrieved: {max_x}x{max_y}")
        spreadsheet = self.spreadsheets.get(
            spreadsheetId=config.SPREADSHEET_ID,
            ranges=[target_range],
            includeGridData=False  # values already fetched
        ).execute()
        self.logger.info(
            f"Spreadsheet {spreadsheet['properties']['title']} retrieved:" +
            f"Sheets({len(spreadsheet['sheets'])}): " +
            f"> {sheet['properties']['title']}"
            for sheet in spreadsheet['sheets']
        )

        # get target sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == target_title:
                target_sheet = sheet
                break

        if target_sheet is None:
            raise ValueError(f"Target sheet {target_title} not found")

        self.logger.info(
            f"Target sheet: {target_sheet['properties']['title']}" +
            f"> Sheet index: ({target_sheet['properties']['index']})" +
            f"> Sheet merges: ({len(target_sheet['merges'])})"
        )

        self.logger.info("Merging cells")

        for merge in target_sheet["merges"]:
            x0 = merge["startRowIndex"]
            y0 = merge["startColumnIndex"]
            x1 = merge["endRowIndex"]
            y1 = merge["endColumnIndex"]

            if x0 < max_x and y0 < max_y:
                df.iloc[x0: x1, y0: y1] = df.iloc[x0][y0]

        df.fillna('', inplace=True)

        self.logger.info("Cells merged")
        self.logger.info("Filling empty cells")
        for y in range(1, max_y):
            course_name = df.iloc[0, y]
            if course_name == '':
                df.iloc[0, y] = df.iloc[0, y - 1]
                self.logger.info(f"> Filled empty cell in courses line: {y}")
        self.logger.info("Empty cells filled")
        self.logger.info("Dataframe ready")
        return df

    def refactor_course_df(
            self,
            course_df: pd.DataFrame,
            group_names
    ) -> pd.DataFrame:
        """
        Refactor course DataFrame to get a DataFrame with one cell
        corresponding to pair (timeslot, group), to one event.
        @param course_df: DataFrame to refactor - multiple cells per event(
        same timeslot and group). Columns: Time, [groups_names].
        @type course_df: pd.DataFrame
        @param group_names: List of group names.
        @type group_names: list[str]
        @return DataFrame with one cell per event. Columns: Time,
        [*groups_names]
        :rtype: pd.DataFrame
        """
        course_df.columns = ["Time", *group_names]
        course_df.set_index("Time", inplace=True)

        course_df = course_df.groupby("Time").agg(
            lambda intersecting_cells: list(
                filter(lambda desc: desc.strip(), intersecting_cells)
            )
        )

        course_df.replace(r'^\s*$', '', regex=True, inplace=True)
        course_df.fillna('', inplace=True)

        return course_df

    def parse_df(self, df: pd.DataFrame) -> dict:
        """
        Parse DataFrame to dict with separation by groups.
        @param df: DataFrame to parse.
        @type df: pd.DataFrame
        @return Dict with groups and their lessons. Day of week is a first
        key. Group is a second key.
        :rtype: dict
        """

        self.logger.debug("Parsing dataframe to separation by days|groups...")
        self.logger.info("Get 'week' indexes...")
        week_column = df.iloc[:, 0]
        week_mask = week_column.isin(config.DAYS).values
        week_indexes = np.argwhere(week_mask).flatten().tolist()
        self.logger.info(f"> Found {len(week_indexes)} indexes:")

        self.logger.debug("Separating by days...")
        max_x, max_y = df.shape
        separation_by_days = defaultdict(dict)
        courses_line = df.iloc[0, 1:]
        groups_line = df.iloc[1, 1:]

        for start_x, end_x in pairwise(week_indexes + [max_x]):
            day_name = week_column[start_x]
            self.logger.info(f"> Separating day {day_name}")
            week_df = df.iloc[start_x:end_x]
            day_row = week_df.iloc[0]
            day_mask = day_row.isin(config.DAYS).values
            day_indexes = np.argwhere(day_mask).flatten().tolist()

            for start_y, end_y in pairwise(day_indexes + [max_y]):
                course_name = courses_line.iloc[start_y]
                self.logger.info(f">> Separating course {course_name}")
                group_names = groups_line.iloc[start_y:end_y]
                group_names = group_names[group_names != ''].values
                course_df = week_df.iloc[1:, start_y: end_y]
                course_df = self.refactor_course_df(course_df, group_names)
                separation_by_days[day_name][course_name] = course_df

        return separation_by_days


def convert_course_df(
        output_dict: dict,
        course_df: pd.DataFrame,
        course_name: str,
        dtstart: datetime,
        dtstamp: datetime,
        rrule: dict
):
    for timeslot, by_groups in course_df.iterrows():  # type: str, dict
        for name, event_lines in by_groups.items():  # type: str, list[str]
            formatted_group_name = format_group_name(name)
            if event_lines:
                group_dict = output_dict[formatted_group_name]
                vevent = icalendar.Event()
                event_parts = iter(
                    map(
                        lambda line: re.sub(r'\s+', ' ', line),
                        event_lines
                    )
                )
                summary = next(event_parts, '')

                if any(
                        classname.lower() in summary.lower() for classname in
                        config.IGNORING_CLASSES
                ):
                    print(f"Skipping {summary}")
                    continue

                description = next(event_parts, '')
                if description:
                    description = f"{description}\n"
                description += formatted_group_name
                location = next(event_parts, '')

                start_delta, end_delta = map(
                    lambda t: datetime.strptime(t, '%H:%M').time(),
                    timeslot.split("-")
                )

                event_start = datetime.combine(dtstart, start_delta)
                event_end = datetime.combine(dtstart, end_delta)

                vevent['summary'] = summary
                vevent['description'] = description
                vevent['location'] = location
                vevent['dtstart'] = event_start.strftime("%Y%m%dT%H%M%S")
                vevent['dtend'] = event_end.strftime("%Y%m%dT%H%M%S")
                vevent['dtstamp'] = dtstamp.strftime("%Y%m%dT%H%M%S")
                vevent['uid'] = str(
                    uuid4()
                ) + "@innohassle.campus.innopolis.university"

                vevent.add(
                    'rrule',
                    rrule
                )
                group_dict["calendar"].add_component(vevent)
                group_dict["group_name"] = formatted_group_name
                group_dict["course_name"] = format_course_name(course_name)


def convert_separation(
        separation_by_days: dict,
        very_first_date: datetime.date,
        very_last_date: datetime.date
) -> defaultdict[icalendar.Calendar]:
    """
    Convert separation by days to icalendar.Calendar without calendar
    properties(only vevents).

    @param separation_by_days: separation by groups.
    @type separation_by_days: dict
    @param very_first_date: First date of the schedule.
    @type very_first_date: datetime.date
    @param very_last_date: Last date of the schedule.
    @type very_last_date: datetime.date
    @return Dict with icalendar.Calendar for each group.
    :rtype: defaultdict[icalendar.Calendar]
    """
    print("Parsing into ics...")
    now_dtstamp = datetime.now()
    calendars_dict = defaultdict(lambda: {"calendar": icalendar.Calendar()})

    for day_name, separation_by_courses in separation_by_days.items():
        print(f"> Parsing day {day_name}")
        weekday_dtstart = nearest_weekday(
            very_first_date,
            weekday_converter[day_name]
        )

        for course_name, course_df in separation_by_courses.items():
            print(f">> Parsing course {course_name}")
            rrule = get_weekday_rrule(very_last_date)
            convert_course_df(
                calendars_dict,
                course_df,
                course_name,
                weekday_dtstart,
                now_dtstamp,
                rrule
            )

    return calendars_dict


def nearest_weekday(date, day):
    """
    Returns the date of the next given weekday after
    the given date. For example, the date of next Monday.

    NB: if it IS the day we're looking for, this returns 0.
    consider then doing onDay(foo, day + 1).
    """
    days = (day - date.weekday() + 7) % 7
    return date + timedelta(days=days)


weekday_converter = {
    'MONDAY': 0,
    'TUESDAY': 1,
    'WEDNESDAY': 2,
    'THURSDAY': 3,
    'FRIDAY': 4,
    'SATURDAY': 5,
    'SUNDAY': 6
}

symbol_translation = str.maketrans(
    "АВЕКМНОРСТУХ",
    "ABEKMHOPCTYX",
    ' \n\t'
)

remove_pattern = re.compile(r"\(.*\)")


def format_group_name(dirt_group_name: str) -> str:
    dirt_group_name = dirt_group_name.upper()
    dirt_group_name = dirt_group_name.translate(symbol_translation)
    dirt_group_name = remove_pattern.sub('', dirt_group_name)
    dirt_group_name = dirt_group_name.strip()
    return dirt_group_name


def format_course_name(dirt_course_name: str) -> str:
    dirt_course_name = dirt_course_name.strip()
    return dirt_course_name


def get_weekday_rrule(end_date):
    return {
        'FREQ': 'WEEKLY',
        'INTERVAL': 1,
        'UNTIL': end_date,
    }


def process_target_schedule(target_id):
    df = parser.get_clear_df(
        spreadsheet_id=config.SPREADSHEET_ID,
        target_range=config.TARGET_RANGES[target_id],
        target_title=config.TARGET_SHEET_TITLES[target_id]
    )
    separation_by_days = parser.parse_df(df)

    from_date = datetime.fromisoformat(
        config.RECURRENCE[target_id]["start"]
    ).date()
    until_date = datetime.fromisoformat(
        config.RECURRENCE[target_id]["end"]
    ).date()

    calendars = convert_separation(
        separation_by_days,
        from_date,
        until_date
    )

    return calendars


if __name__ == '__main__':
    parser = AcademicParser()
    calendars_dict = process_target_schedule(0)
    calendars_dict_second = process_target_schedule(1)
    calendars_dict_third = process_target_schedule(2)

    # unite calendars
    for group_name, calendar_dict in calendars_dict_second.items():
        for event in calendar_dict["calendar"].walk('vevent'):
            calendars_dict[group_name]["calendar"].add_component(event)

    for group_name, calendar_dict in calendars_dict_third.items():
        for event in calendar_dict["calendar"].walk('vevent'):
            calendars_dict[group_name]["calendar"].add_component(event)

    calendars = {
        "filters": [{
            "title": "Course",
            "alias": "course"
        }],
        "title": "Academic",
        "calendars": []
    }

    for group_name, calendar_dict in calendars_dict.items():
        print(f"Writing {group_name}...")
        calendar = calendar_dict["calendar"]
        calendar['prodid'] = '-//one-zero-eight//InNoHassle Calendar'
        calendar['version'] = '2.0'
        calendar['x-wr-calname'] = group_name
        calendar['x-wr-caldesc'] = 'Generated by InNoHassle Calendar'
        calendar['x-wr-timezone'] = config.TIMEZONE

        file_name = f"{group_name}.ics"
        calendars["calendars"].append(
            {
                "name": group_name,
                "course": calendar_dict["course_name"],
                "file": "academic/" + file_name
            }
        )
        with open(
                PARSER_PATH / config.SAVE_PATH / file_name,
                'wb'
        ) as f:
            f.write(calendar.to_ical())

    # create a new .json file with information about calendar
    with open(
            PARSER_PATH / config.SAVE_JSON_PATH,
            "w"
    ) as f:

        json.dump(calendars, f, indent=4)
