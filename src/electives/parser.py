import io
import re
from collections.abc import Generator
from datetime import datetime
from itertools import groupby, pairwise
from typing import TypedDict

import numpy as np
import openpyxl
import pandas as pd
import requests
from openpyxl.utils import coordinate_to_tuple, get_column_letter

from src.electives.config import Target
from src.electives.config import electives_config as config
from src.electives.models import Elective, ElectiveCell, ElectiveEvent
from src.logging_ import logger
from src.processors.regex import prettify_string
from src.utils import get_current_year

BRACKETS_PATTERN = re.compile(r"\((.*?)\)")


class ElectiveParser:
    """
    Elective parser class
    """

    def __init__(self):
        self.session = requests.Session()

    def get_clear_dataframes_from_xlsx(self, xlsx_file: io.BytesIO, targets: list[Target]) -> dict[str, pd.DataFrame]:
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
        dfs = {key: value for key, value in dfs.items()}

        for target in targets:
            logger.debug(f"Processing sheet: {target.sheet_name}")
            df = dfs[target.sheet_name]
            # -------- Select range --------
            (min_row, min_col, max_row, max_col) = self.auto_detect_range(df, xlsx_file, target.sheet_name)
            df = df.iloc[min_row : max_row + 1, min_col : max_col + 1]
            # -------- Set time column as index --------
            df = ElectiveParser.set_time_column_as_index(df)
            # -------- Strip all values --------
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            # -------- Fill empty cells --------
            df = df.replace(r"^\s*$", np.nan, regex=True)
            # -------- Exclude nan rows --------
            df = df.dropna(how="all")
            # -------- Strip, translate and remove trailing spaces --------
            df = df.map(prettify_string)
            # -------- Update dataframe --------
            dfs[target.sheet_name] = df
        logger.debug("Dataframes ready")
        return dfs

    
    def auto_detect_range(
        self, sheet_df: pd.DataFrame, xlsx_file: io.BytesIO, sheet_name: str
    ) -> tuple[int, int, int, int]:
        """
        :return: tuple of (min_row, min_col, max_row, max_col)
        """

        weekdays = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

        # find all columns named as weekday by checking the first row
        first_row = sheet_df.iloc[0]
        weekday_columns_index = [i for i, value in enumerate(first_row) if isinstance(value, str) and value.upper().strip() in weekdays]
        assert len(weekday_columns_index) == len(weekdays), "Weekday columns not found"
        rightmost_column_index = max(weekday_columns_index)
        leftmost_column_index = min(weekday_columns_index) - 1
        logger.info(f"Rightmost column index: {get_column_letter(rightmost_column_index + 1)}")
        last_row_index = self.get_last_row_index(xlsx_file, sheet_name)
        target_range = f"{get_column_letter(leftmost_column_index + 1)}1:{get_column_letter(rightmost_column_index + 1)}{last_row_index}"
        logger.info(f"Target range: {target_range}")
        return (0, leftmost_column_index, last_row_index, rightmost_column_index)

    def get_last_row_index(self, xlsx_file: io.BytesIO, sheet_name: str) -> int:
        wb = openpyxl.load_workbook(xlsx_file)
        sheet = wb[sheet_name]
        return sheet.max_row

    def get_xlsx_file(self, spreadsheet_id: str) -> io.BytesIO:
        """
        Export xlsx file from Google Sheets and return it as BytesIO object.

        :param spreadsheet_id: id of Google Sheets spreadsheet
        :return: xlsx file as BytesIO object
        """
        # ------- Get data from Google Sheets -------
        logger.debug("Getting dataframe from Google Sheets...")
        # ------- Create url for export -------
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        export_url = spreadsheet_url + "/export?format=xlsx"
        # ------- Export xlsx file -------
        logger.debug(f"Exporting from URL: {export_url}")
        response = self.session.get(export_url)
        logger.debug(f"Response status: {response.status_code}")
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
    def set_time_column_as_index(cls, df: pd.DataFrame, column: int = 0) -> pd.DataFrame:
        """
        Set time column as index and process it to datetime format

        :param df: dataframe to process
        :type df: pd.DataFrame
        :param column: column to set as index, defaults to 0
        :type column: int, optional
        """
        # "9:00-10:30" -> datetime.time(9, 0), datetime.time(10, 30)
        df[column] = df[column].apply(lambda x: ElectiveParser.proces_time_cell(x) if isinstance(x, str) else x)
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
        df.loc[index] = df.loc[index].apply(lambda x: ElectiveParser.process_date_cell(x) if isinstance(x, str) else x)
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
            cell = str(get_current_year()) + " " + cell
            dtime = datetime.strptime(cell, "%Y %B %d")
            return dtime.date()
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

        logger.debug("Parsing dataframe to separation by days|groups...")
        logger.debug("Get 'week' indexes...")
        # find indexes of row with "Week *"
        where = df.index.str.contains(r"Week \d", na=False)
        week_indexes = df.index[where]
        week_locations = df.index.get_indexer_for(week_indexes).tolist()
        logger.debug(f"> Found {len(week_locations)} weeks")

        max_x, _ = df.shape
        week_locations += [max_x]  # add last index
        # split dataframe by week indexes
        dfs = []
        for start, end in pairwise(week_locations):
            week = df.index[start]
            logger.debug(f"Processing week: {week}... From ({start}) to ({end})")
            week_df: pd.DataFrame = df.iloc[start:end].copy()
            # ----- Set date row as header -----
            week_df = ElectiveParser.set_date_row_as_header(week_df)
            dfs.append(week_df)
        return dfs

    @classmethod
    def parse_df(cls, df: pd.DataFrame) -> Generator[ElectiveEvent, None, None]:
        """
        Parse dataframe with schedule

        :param df: dataframe with schedule
        :type df: pd.DataFrame
        :return: parsed events
        """

        _elective_short_name = [e.short_name for e in config.electives]
        _elective_line_pattern = re.compile(r"(?P<elective_alias>" + "|".join(_elective_short_name) + r")")

        def process_line(line: str) -> ElectiveCell | str:
            """
            Process line of the dataframe

            :param line: line to process
            :type line: str
            :return: ElectiveCell or original line
            :rtype: ElectiveCell | str
            """
            if pd.isna(line):
                return line
            line = line.strip()
            # find all matches in the string and split by them
            matches = _elective_line_pattern.finditer(line)
            # get substrings
            breaks = [m.start() for m in matches]
            substrings = [line[i:j] for i, j in zip(breaks, breaks[1:] + [None])]
            substrings = [line[: breaks[0]]] + substrings
            substrings = filter(len, substrings)
            substrings = map(str.strip, substrings)
            return ElectiveCell(original=list(substrings))

        df = df.map(lambda x: process_line(x) if isinstance(x, str) else x)

        for date, date_column in df.items():
            date: datetime.date
            for timeslot, cell in date_column.items():
                timeslot: tuple[datetime.time, datetime.time]

                if isinstance(cell, ElectiveCell):
                    yield from cell.generate_events(date, timeslot)


class Separation(TypedDict):
    name: str
    events: list[ElectiveEvent]


def convert_separation(events: list[ElectiveEvent]) -> dict[str, Separation]:
    """
    Convert list of events to dict with separation by Elective and group.

    :param events: list of events to convert
    :type events: list[ElectiveEvent]
    :return: dict with separation by Elective and group
    :rtype: dict[str, list[str, list[ElectiveEvent]]] (name, events)
    """
    output: dict[str, Separation] = dict()

    # # by groups of elective
    # for (elective, group), _events in groupby(events, lambda e: (e.elective, e.group)):
    #     elective: Elective
    #     if group is None:
    #         continue  # cal = output[elective.alias]
    #     else:
    #         cal = output[f"{elective.alias}-{group}"]
    #     cal: list[ElectiveEvent]
    #     cal.extend(_events)

    # only by Elective
    for elective, _events in groupby(events, lambda e: e.elective):
        elective: Elective
        if elective.alias not in output:
            output[elective.alias] = Separation(
                name=elective.name or "",
                events=list(_events),
            )
        else:
            output[elective.alias]["events"].extend(_events)

    return dict(output)
