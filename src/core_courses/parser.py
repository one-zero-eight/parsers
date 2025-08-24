import io
from collections import defaultdict
from collections.abc import Generator
from datetime import datetime
from itertools import pairwise

import numpy as np
import openpyxl
import pandas as pd
import requests
from openpyxl.utils import get_column_letter

from src.constants import WEEKDAYS
from src.core_courses.config import Target
from src.core_courses.models import CoreCourseCell, CoreCourseEvent
from src.logging_ import logger
from src.processors.regex import prettify_string
from src.utils import split_range_to_xy


class CoreCoursesParser:
    """
    Elective parser class
    """

    #
    # credentials: Credentials
    # """ Google API credentials object """

    def __init__(self):
        self.session = requests.Session()

    def get_clear_dataframes_from_xlsx(
        self, xlsx_file: io.BytesIO, target_sheet_names: list[str]
    ) -> tuple[dict[str, pd.DataFrame], dict]:
        """
        Get data from xlsx file and return it as a DataFrame with merged
        cells and empty cells in the course row filled by left value.

        :return: dataframes with merged cells and empty cells filled
        :rtype: dict[str, pd.DataFrame]
        """
        # ------- Read xlsx file into dataframes -------
        dfs = pd.read_excel(xlsx_file, engine="openpyxl", sheet_name=None, header=None)
        # ------- Clean up dataframes -------
        merged_ranges: dict[str, list[tuple[int, int, int, int]]] = defaultdict(list)
        for target_sheet_name in target_sheet_names:
            df = dfs[target_sheet_name]
            # -------- Select range --------
            (min_row, min_col, max_row, max_col) = self.auto_detect_range(df, xlsx_file, target_sheet_name)
            df = df.iloc[min_row : max_row + 1, min_col : max_col + 1]
            # -------- Fill merged cells with values --------
            merged_ranges[target_sheet_name] = self.merge_cells(df, xlsx_file, target_sheet_name)
            # -------- Fill empty cells --------
            df = df.replace(r"^\s*$", np.nan, regex=True)
            # -------- Strip, translate and remove trailing spaces --------
            df = df.map(prettify_string)
            # -------- Update dataframe --------
            dfs[target_sheet_name] = df

        return dfs, merged_ranges

    def auto_detect_range(
        self, sheet_df: pd.DataFrame, xlsx_file: io.BytesIO, sheet_name: str
    ) -> tuple[int, int, int, int]:
        """
        :return: tuple of (min_row, min_col, max_row, max_col)
        """
        time_columns_index = self.get_time_columns(sheet_df)
        logger.info(f"Time columns: {[get_column_letter(col + 1) for col in time_columns_index]}")
        # -------- Get rightmost column index --------
        rightmost_column_index = self.get_rightmost_column_index(xlsx_file, sheet_name, time_columns_index)
        logger.info(f"Rightmost column index: {get_column_letter(rightmost_column_index + 1)}")
        last_row_index = self.get_last_row_index(xlsx_file, sheet_name)
        target_range = f"A1:{get_column_letter(rightmost_column_index + 1)}{last_row_index}"
        logger.info(f"Target range: {target_range}")
        return (0, 0, last_row_index, rightmost_column_index)

    def get_time_columns(self, sheet_df: pd.DataFrame) -> list[int]:
        # find columns where presents "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY"
        time_columns = []
        for column in sheet_df.columns:
            values_in_column = sheet_df[column].values
            if all(weekday in values_in_column for weekday in WEEKDAYS[:-1]):
                time_columns.append(column)
        return time_columns

    def get_rightmost_column_index(self, xlsx_file: io.BytesIO, sheet_name: str, time_columns: list[int]) -> int:
        # Column after time columns that has no borders formatting

        wb = openpyxl.load_workbook(xlsx_file)
        sheet = wb[sheet_name]
        last_time_column = time_columns[-1]

        next_column = last_time_column + 1
        cell = sheet.cell(row=1, column=next_column + 1)

        # Check if cell has no borders
        has_no_border = (
            (cell.border is None or cell.border.right is None or cell.border.right.style is None)
            and (cell.border is None or cell.border.top is None or cell.border.top.style is None)
            and (cell.border is None or cell.border.bottom is None or cell.border.bottom.style is None)
        )

        if has_no_border:
            return next_column - 1
        else:
            # Continue searching for column with no border
            for col in range(next_column + 1, sheet.max_column + 1):
                cell = sheet.cell(row=1, column=col + 1)
                if (
                    (cell.border is None or cell.border.right is None or cell.border.right.style is None)
                    and (cell.border is None or cell.border.top is None or cell.border.top.style is None)
                    and (cell.border is None or cell.border.bottom is None or cell.border.bottom.style is None)
                ):
                    return col - 1
            return next_column  # fallback

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
        logger.info(f"Exporting from URL: {export_url}")
        response = self.session.get(export_url)
        logger.info(f"Response status: {response.status_code}")
        response.raise_for_status()
        # ------- Return xlsx file as BytesIO object -------
        return io.BytesIO(response.content)

    @classmethod
    def merge_cells(cls, df: pd.DataFrame, xlsx: io.BytesIO, target_sheet_name: str) -> list[tuple[int, int, int, int]]:
        """
        Merge cells in dataframe

        :param df: Dataframe to process
        :param xlsx: xlsx file with data
        :param target_sheet_name: sheet to process
        :return: list of merged ranges: (min_row, min_col, max_row, max_col)
        """
        xlsx.seek(0)
        ws = openpyxl.load_workbook(xlsx)
        sheet = ws[target_sheet_name]
        merged_ranges = []
        # ------- Merge cells -------
        for merged_range in sheet.merged_cells.ranges:
            min_col, min_row, max_col, max_row = merged_range.bounds
            min_col = min_col - 1
            min_row = min_row - 1
            max_col = max_col - 1
            max_row = max_row - 1
            value = df.iloc[min_row, min_col]

            df.iloc[min_row : max_row + 1, min_col : max_col + 1] = value
            merged_ranges.append((min_row, min_col, max_row, max_col))
        return merged_ranges

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
    def set_weekday_and_time_as_index(cls, df: pd.DataFrame, column: int = 0) -> pd.DataFrame:
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
        df_column.ffill(inplace=True)

        # ----- Process weekday ------ #
        # get indexes of weekdays
        weekdays_indexes = [i for i, cell in enumerate(df_column.values) if cell in WEEKDAYS]

        # create index mapping for weekdays [None, None, "MONDAY", "MONDAY", ...]
        index_mapping = pd.Series(index=df_column.index, dtype=object)
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
        multiindex = pd.MultiIndex.from_arrays([index_mapping, df_column], names=["weekday", "time"])
        # set multiindex as index
        df.set_index(multiindex, inplace=True)
        # drop rows with weekday
        df.drop("delete", inplace=True, level=0)
        return df

    @classmethod
    def set_course_and_group_as_header(cls, df: pd.DataFrame, rows: tuple = (0, 1)) -> pd.DataFrame:
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
        with pd.option_context("future.no_silent_downcasting", True):
            df_header = df_header.ffill(axis=1)
        multiindex = pd.MultiIndex.from_arrays(df_header.values, names=["course", "group"])
        df.columns = multiindex
        return df

    @classmethod
    def split_df_by_courses(cls, df: pd.DataFrame, time_columns: list[int]) -> list[pd.DataFrame]:
        """
        Split dataframe by "Week *" rows

        :param time_columns: list of columns(pd) with time and weekday
        :param df: dataframe to split
        :type df: pd.DataFrame
        :return: list of dataframes with locators
        :rtype: list[pd.DataFrame, ExcelToPandasLocator]
        """

        logger.debug("Parsing dataframe to separation by course|groups...")
        logger.info("Get indexes of time columns...")

        time_columns_indexes = time_columns
        logger.info(f"Time columns indexes: {time_columns_indexes}")

        # split dataframe by found columns
        _, max_y = df.shape
        split_indexes = time_columns_indexes + [max_y]

        split_dfs = []

        for i, (start, end) in enumerate(pairwise(split_indexes)):
            logger.info(f"Splitting dataframe by columns {start}:{end}")
            split_df = df.iloc[:, start:end].copy()
            split_dfs.append(split_df)
        return split_dfs

    @classmethod
    def generate_events_from_processed_column(
        cls,
        processed_column: pd.Series,
        target: Target,
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
            )

            if event is None:
                continue

            yield event
