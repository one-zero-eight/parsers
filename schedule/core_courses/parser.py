import io
import logging
import re
from collections import defaultdict
from datetime import datetime
from itertools import pairwise, groupby
from typing import Collection, Generator
from zipfile import ZipFile

import icalendar
import numpy as np
import pandas as pd
import requests
from google.oauth2.credentials import Credentials

from schedule.core_courses.config import core_courses_config as config
from schedule.core_courses.models import CoreCourseEvent, CoreCourseCell
from schedule.processors.regex import prettify_string
from schedule.utils import *


class CoreCoursesParser:
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

    def get_clear_dataframes_from_xlsx(
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
            self.logger.info(f"Processing sheet: '{target.sheet_name}'")
            df = dfs[target.sheet_name]
            # -------- Fill merged cells with values --------
            CoreCoursesParser.merge_cells(df, xlsx_file, target.sheet_name)
            # -------- Select range --------
            df = CoreCoursesParser.select_range(df, target.range)
            # -------- Fill empty cells --------
            df = df.replace(r"^\s*$", np.nan, regex=True)
            # -------- Strip, translate and remove trailing spaces --------
            df = df.applymap(prettify_string)
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
    def merge_cells(cls, df: pd.DataFrame, xlsx: io.BytesIO, target_sheet_name: str):
        """
        Merge cells in dataframe

        :param df: Dataframe to process
        :param xlsx: xlsx file with data
        :param target_sheet_name: sheet to process
        """
        xlsx.seek(0)
        xlsx_zipfile = ZipFile(xlsx)
        sheets = get_sheets(xlsx_zipfile)
        target_sheet_id = None
        for sheet_id, sheet_name in sheets.items():
            if target_sheet_name in sheet_name:
                target_sheet_id = sheet_id
                break
        sheet = get_sheet_by_id(xlsx_zipfile, target_sheet_id)
        merged_ranges = get_merged_ranges(sheet)

        # ------- Merge cells -------
        for merged_range in merged_ranges:
            (start_row, start_col), (end_row, end_col) = split_range_to_xy(merged_range)
            # get value from top left cell
            value = df.iloc[start_row, start_col]
            # fill merged cells with value
            df.iloc[start_row : end_row + 1, start_col : end_col + 1] = value

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
        (start_row, start_col), (end_row, end_col) = split_range_to_xy(target_range)
        return df.iloc[
            start_row : end_row + 1,
            start_col : end_col + 1,
        ]

    @classmethod
    def set_weekday_and_time_as_index(
        cls, df: pd.DataFrame, column: int = 0
    ) -> pd.DataFrame:
        """
        Set time column as index and process it to datetime format

        :param df: dataframe to process
        :type df: pd.DataFrame
        :param column: column to set as index, defaults to 0
        :type column: int, optional
        """

        # get column view and iterate over it
        df_column = df.iloc[:, column]
        df_column: pd.Series
        # drop column
        df.drop(df.columns[column], axis=1, inplace=True)
        # fill nan values with previous value
        df_column.fillna(method="ffill", inplace=True)

        # ----- Process weekday ------ #
        # get indexes of weekdays
        weekdays_indexes = [
            i for i, cell in enumerate(df_column.values) if cell in config.WEEKDAYS
        ]

        # create index mapping for weekdays [None, None, "MONDAY", "MONDAY", ...]
        index_mapping = pd.Series(index=df_column.index)
        last_index = len(df_column)
        for start, end in pairwise(weekdays_indexes + [last_index]):
            index_mapping.iloc[start] = "delete"
            index_mapping.iloc[start + 1 : end] = df_column[start]

        # ----- Process time ------ #
        # matched r"\d{1,2}:\d{2}-\d{1,2}:\d{2}" regex

        matched = df_column[df_column.str.match(r"\d{1,2}:\d{2}-\d{1,2}:\d{2}")]

        for i, cell in matched.items():
            # "9:00-10:30" -> datetime.time(9, 0), datetime.time(10, 30)
            start, end = cell.split("-")
            df_column.loc[i] = (
                datetime.strptime(start, "%H:%M").time(),
                datetime.strptime(end, "%H:%M").time(),
            )

        # create multiindex from index mapping and time column
        multiindex = pd.MultiIndex.from_arrays(
            [index_mapping, df_column], names=["weekday", "time"]
        )
        # set multiindex as index
        df.set_index(multiindex, inplace=True)
        # drop rows with weekday
        df.drop("delete", inplace=True, level=0)
        return df

    @classmethod
    def set_course_and_group_as_header(
        cls, df: pd.DataFrame, rows: tuple = (0, 1)
    ) -> pd.DataFrame:
        """
        Set course and group as header

        :param df: dataframe to process
        :type df: pd.DataFrame
        :param rows: row to set as columns, defaults to (0, 1)
        :type rows: tuple, optional
        """
        # ------- Set course and group as header -------
        # get rows with course and group
        df_header = df.iloc[rows[0] : rows[1] + 1]
        # drop rows with course and group
        df.drop(list(rows), inplace=True)
        df.reset_index(drop=True, inplace=True)
        # fill nan values with previous value
        df_header = df_header.fillna(method="ffill", axis=1)
        multiindex = pd.MultiIndex.from_arrays(
            df_header.values, names=["course", "group"]
        )
        df.columns = multiindex
        return df

    @classmethod
    def split_df_by_courses(
        cls, df: pd.DataFrame, time_columns: list[int]
    ) -> list[pd.DataFrame]:
        """
        Split dataframe by "Week *" rows

        :param time_columns: list of columns(pd) with time and weekday
        :param df: dataframe to split
        :type df: pd.DataFrame
        :return: list of dataframes with locators
        :rtype: list[pd.DataFrame, ExcelToPandasLocator]
        """

        cls.logger.debug("Parsing dataframe to separation by course|groups...")
        cls.logger.info("Get indexes of time columns...")

        time_columns_indexes = time_columns
        cls.logger.info(f"Time columns indexes: {time_columns_indexes}")

        # split dataframe by found columns
        _, max_y = df.shape
        split_indexes = time_columns_indexes + [max_y]

        split_dfs = []

        for i, (start, end) in enumerate(pairwise(split_indexes)):
            cls.logger.info(f"Splitting dataframe by columns {start}:{end}")
            split_df = df.iloc[:, start:end].copy()
            split_dfs.append(split_df)
        return split_dfs

    @classmethod
    def generate_events_from_processed_column(
        cls,
        processed_column: pd.Series,
        target: config.Target,
    ) -> Generator[CoreCourseEvent, None, None]:
        """
        Generate events from processed cells

        :param target: target to generate events for (needed for start and end dates)
        :param processed_column: series with processed cells (CoreCourseCell),
         multiindex with (weekday, timeslot) and (course, group) as name
        :return: generator of events
        """
        # -------- Iterate over processed cells --------
        (course, group) = processed_column.name
        course: str
        group: str

        for (weekday, timeslot), cell in processed_column.items():
            if cell is None:
                continue
            cell: CoreCourseCell
            weekday: str
            timeslot: tuple[datetime.time, datetime.time]

            event = cell.get_event(
                weekday=weekday,
                timeslot=timeslot,
                course=course,
                group=group,
                target=target,
                return_none=True,
            )

            if event is None:
                continue

            yield event
