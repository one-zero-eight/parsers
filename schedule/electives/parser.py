import io
import logging
import re
from collections import defaultdict
from datetime import datetime
from itertools import pairwise, groupby
from typing import Collection

import numpy as np
import pandas as pd
import requests
from google.oauth2.credentials import Credentials
from openpyxl.utils.cell import coordinate_to_tuple

from schedule.electives.config import electives_config as config
from schedule.electives.models import Elective, ElectiveEvent
from schedule.utils import *

BRACKETS_PATTERN = re.compile(r"\((.*?)\)")


class ElectiveParser:
    """
    Elective parser class
    """

    credentials: Credentials
    """ Google API credentials object """
    logger = logging.getLogger(__name__ + "." + "Parser")
    """ Logger object """

    def __init__(self):
        self.credentials = get_credentials(
            credentials_path=config.CREDENTIALS_PATH,
            token_path=config.TOKEN_PATH,
            scopes=config.API_SCOPES,
        )
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {self.credentials.token}"}
        )

    def clear_df(
        self, xlsx_file: io.BytesIO, targets: list[config.Target]
    ) -> dict[str, pd.DataFrame]:
        """
        Get data from xlsx file and return it as a DataFrame with merged
        cells and empty cells in the course row filled by left value.

        :param xlsx_file: xlsx file with data
        :type xlsx_file: io.BytesIO
        :param targets: list of targets to get data from (sheets and ranges)
        :type targets: list[config.Target]

        :return: dataframes with merged cells and empty cells filled
        :rtype: dict[str, pd.DataFrame]
        """
        # ------- Read xlsx file into dataframes -------
        dfs = pd.read_excel(xlsx_file, engine="openpyxl", sheet_name=None, header=None)
        # ------- Clean up dataframes -------
        dfs = {key.strip(): value for key, value in dfs.items()}

        for target in targets:
            self.logger.info(f"Processing sheet: {target.sheet_name}")
            df = dfs[target.sheet_name]
            # -------- Select range --------
            df = ElectiveParser.select_range(df, target.range)
            # -------- Set time column as index --------
            df = ElectiveParser.set_time_column_as_index(df)
            # -------- Strip all values --------
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # -------- Fill empty cells --------
            df = df.replace(r"^\s*$", np.nan, regex=True)
            # -------- Exclude nan rows --------
            df = df.dropna(how="all")
            # -------- Update dataframe --------
            dfs[target.sheet_name] = df
        self.logger.info("Dataframes ready")
        return dfs

    def get_xlsx_file(self, spreadsheet_id: str) -> io.BytesIO:
        """
        Export xlsx file from Google Sheets and return it as BytesIO object.

        :param spreadsheet_id: id of Google Sheets spreadsheet
        :return: xlsx file as BytesIO object
        """
        # ------- Get data from Google Sheets -------
        self.logger.debug("Getting dataframe from Google Sheets...")
        # ------- Create url for export -------
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        export_url = spreadsheet_url + "/export?format=xlsx"
        # ------- Export xlsx file -------
        self.logger.info(f"Exporting from URL: {export_url}")
        response = self.session.get(export_url)
        self.logger.info(f"Response status: {response.status_code}")
        response.raise_for_status()
        # ------- Return xlsx file as BytesIO object -------
        return io.BytesIO(response.content)

    @classmethod
    def select_range(cls, df: pd.DataFrame, target_range: str) -> pd.DataFrame:
        """
        Select range from dataframe

        :param df: dataframe to process
        :type df: pd.DataFrame
        :param target_range: range to select
        :type target_range: str
        :return: selected range
        :rtype: pd.DataFrame
        """
        start, end = target_range.split(":")
        start_row, start_col = coordinate_to_tuple(start)
        end_row, end_col = coordinate_to_tuple(end)
        return df.iloc[
            start_row - 1 : end_row,
            start_col - 1 : end_col,
        ]

    @classmethod
    def set_time_column_as_index(
        cls, df: pd.DataFrame, column: int = 0
    ) -> pd.DataFrame:
        """
        Set time column as index and process it to datetime format

        :param df: dataframe to process
        :type df: pd.DataFrame
        :param column: column to set as index, defaults to 0
        :type column: int, optional
        """
        # "9:00-10:30" -> datetime.time(9, 0), datetime.time(10, 30)
        df[column] = df[column].apply(
            lambda x: ElectiveParser.proces_time_cell(x) if isinstance(x, str) else x
        )
        df.set_index(column, inplace=True)
        df.rename_axis(index="time", inplace=True)
        return df

    @classmethod
    def set_date_row_as_header(cls, df: pd.DataFrame, row: int = 0) -> pd.DataFrame:
        """
        Set date row as columns and process it to datetime format

        :param df: dataframe to process
        :type df: pd.DataFrame
        :param row: row to set as columns, defaults to 0
        :type row: int, optional
        """
        # "June 7" -> datetime.date(current_year, 6, 7)
        index = df.index[row]
        df.loc[index] = df.loc[index].apply(
            lambda x: ElectiveParser.process_date_cell(x) if isinstance(x, str) else x
        )
        df.columns = df.loc[index]
        df.drop(index, inplace=True)
        df.rename_axis(columns="date", inplace=True)
        return df

    @classmethod
    def proces_time_cell(cls, cell: str) -> tuple[datetime.time, datetime.time] | str:
        """
        Process time cell and return tuple of start and end time

        :param cell: cell to process
        :type cell: str
        :return: tuple of start and end time
        :rtype: tuple[datetime.time, datetime.time]
        """
        # "9:00-10:30" -> datetime.time(9, 0), datetime.time(10, 30)
        if re.match(r"\d{1,2}:\d{2}-\d{1,2}:\d{2}", cell):
            start, end = cell.split("-")
            return (
                datetime.strptime(start, "%H:%M").time(),
                datetime.strptime(end, "%H:%M").time(),
            )
        else:
            return cell

    @classmethod
    def process_date_cell(cls, cell: str) -> datetime.date:
        """
        Process date cell and return datetime.date

        :param cell: cell to process
        :type cell: str
        :return: datetime.date
        :rtype: datetime.date
        """
        # "June 7" -> datetime.date(current_year, 6, 7)
        if re.match(r"\w+ \d+", cell):
            dtime = datetime.strptime(cell, "%B %d")
            return dtime.date().replace(year=get_current_year())
        else:
            return cell

    @classmethod
    def split_df_by_weeks(cls, df: pd.DataFrame) -> list[pd.DataFrame]:
        """
        Split dataframe by "Week *" rows

        :param df: dataframe to split
        :type df: pd.DataFrame
        :return: list of dataframes
        :rtype: list[pd.DataFrame]
        """

        cls.logger.debug("Parsing dataframe to separation by days|groups...")
        cls.logger.info("Get 'week' indexes...")
        # find indexes of row with "Week *"
        where = df.index.str.contains(r"Week \d", na=False)
        week_indexes = df.index[where]
        week_locations = df.index.get_indexer_for(week_indexes).tolist()
        cls.logger.info(f"> Found {len(week_locations)} weeks")

        max_x, _ = df.shape
        week_locations += [max_x]  # add last index
        # split dataframe by week indexes
        dfs = []
        for start, end in pairwise(week_locations):
            week = df.index[start]
            cls.logger.info(f"Processing week: {week}... From ({start}) to ({end})")
            week_df: pd.DataFrame = df.iloc[start:end].copy()
            # ----- Set date row as header -----
            week_df = ElectiveParser.set_date_row_as_header(week_df)
            dfs.append(week_df)
        return dfs

    @classmethod
    def parse_df(
        cls, df: pd.DataFrame, electives: Collection[Elective]
    ) -> list[ElectiveEvent]:
        """
        Parse dataframe with schedule

        :param df: dataframe with schedule
        :type df: pd.DataFrame
        :param electives: list of electives to parse
        :type electives: Collection[parser.models.Elective]
        :return: list of parsed events
        :rtype: list[ElectiveEvent]
        """

        # for each cell in day column
        def process_cell(cell: str) -> list[dict[str, str]]:
            """
            Process cell, find events in it and return list of parsed events

            :param cell: cell to process
            :type cell: str
            :return: list of parsed events
            :rtype: list[dict[str, str]]
            """
            #           "BDLD(lec) 312" ->
            #           {"ele"BDLD", "lec", "312"
            #           "PP(lab/group2)303" -> "PP", "lab/group2", "303"

            result: list[dict] = []

            if pd.isna(cell):
                return result

            occurences = cell.strip().split("\n")

            for line in occurences:
                dct = {}
                # find event_type in brackets "BDLD(lec) 312"
                # if brackets in the end of the line, just add to the description

                if end_brackets := re.search(r"\((.*?)\)$", line):
                    dct["notes"] = end_brackets.group(1)
                    line = line.replace(end_brackets.group(0), " ")
                elif middle_brackets := re.search(r"\((.*?)\)", line):
                    event_type = middle_brackets.group(1)
                    event_type = event_type.replace(" ", "")
                    line = line.replace(middle_brackets.group(0), " ")
                    if event_type.startswith("lab"):
                        if "/" in event_type:
                            dct["group"] = event_type.split("/")[1]
                        event_type = "lab"
                    elif not event_type.startswith(("lec", "tut")):
                        dct["group"] = event_type
                        event_type = None

                    dct["event_type"] = event_type

                parts = line.split()

                if len(parts) == 2:
                    event_name, event_location = parts
                else:
                    event_name = parts[0]
                    event_location = None

                elective = next((e for e in electives if e.alias == event_name), None)

                if elective is None:
                    raise ValueError(f"Course not found: {event_name}")

                dct["elective"] = elective
                dct["location"] = event_location

                result.append(dct)
            return result

        # first column is time
        # first row is day in format 'Month Day'
        # first cell is week number

        events = []

        days = [day for day in df.columns if day != ""]
        days: list[datetime.date]
        for day in days:
            # copy column
            day_df = df[day].copy()
            # drop rows with empty cells
            day_df = day_df.dropna()
            day_df = day_df[day_df != ""]
            # for each cell in day column
            for timeslot, cell in day_df.items():
                start_delta, end_delta = timeslot
                cell_events = process_cell(cell)
                event_start = datetime.combine(day, start_delta)
                event_end = datetime.combine(day, end_delta)

                for cell_event in cell_events:
                    event = ElectiveEvent(
                        start=event_start, end=event_end, **cell_event
                    )
                    events.append(event)

        return events


def convert_separation(
    events: list[ElectiveEvent],
) -> dict[str, list[ElectiveEvent]]:
    """
    Convert list of events to dict with separation by Elective and group.

    :param events: list of events to convert
    :type events: list[ElectiveEvent]
    :return: dict with separation by Elective and group
    :rtype: dict[str, list[ElectiveEvent]]
    """
    output = defaultdict(list)
    # group events by Elective and group
    grouping = groupby(events, lambda e: (e.elective, e.group))

    for (elective, group), events in grouping:
        elective: Elective
        if group is None:
            cal = output[elective.alias]
        else:
            cal = output[f"{elective.alias}-{group}"]
        cal: list[ElectiveEvent]
        cal.extend(events)

    return dict(output)
