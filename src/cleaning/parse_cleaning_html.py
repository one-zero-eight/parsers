import io
import re
from collections import defaultdict
from datetime import date, datetime

import bs4
import httpx
import numpy as np
import pandas as pd
import requests
from dateutil import relativedelta

from src.logging_ import logger


def process_dataframe(df: pd.DataFrame, entries: dict[str, list[date]]) -> None:
    # drop columns that contain only NaN
    df = df.dropna(axis="columns", how="all")
    df = df.dropna(axis="index", how="all")
    df = df.reindex()
    # now second row should be [Monday ПОНЕДЕЛЬНИК,Tuesday ВТОРНИК,Wednesday СРЕДА,Thursday ЧЕТВЕРГ,
    # Friday ПЯТНИЦА,Saturday СУББОТА,Sunday ВОСКРЕСЕНЬЕ]
    assert (
        df.iloc[1]
        == [
            "Monday ПОНЕДЕЛЬНИК",
            "Tuesday ВТОРНИК",
            "Wednesday СРЕДА",
            "Thursday ЧЕТВЕРГ",
            "Friday ПЯТНИЦА",
            "Saturday СУББОТА",
            "Sunday ВОСКРЕСЕНЬЕ",
        ]
    ).all(), "Second row should be days of week"

    # first cell should be year
    year = int(df.iloc[0, 0])
    # and all others in first row month - "Сентябрь/September"
    month = datetime.strptime(df.iloc[0, 1].split("/")[-1], "%B").month
    # 3, 5, 7, 9, 11... rows should be days of month (be careful at the end of month and start of month)
    days = df.iloc[2::2, :]
    # drop first, second and days rows
    df = df.drop(df.index[[0, 1, *range(2, len(df), 2)]])
    days = days.astype(int).values.flatten()
    first_day_large_than_15 = days[0] > 15
    # Split the list based on monotonic increasing by one
    split_lists = np.split(days, np.where(np.diff(days) != 1)[0] + 1)
    prev = cur = next = []
    if len(split_lists) == 1:
        cur = split_lists[0]
    elif len(split_lists) == 2:
        if first_day_large_than_15:
            prev, cur = split_lists
        else:
            cur, next = split_lists
    elif len(split_lists) == 3:
        prev, cur, next = split_lists
    else:
        raise ValueError("Too many splits")
    days_as_dates = []
    current_month_date = date(year, month, 1)
    previous_month_date = current_month_date - relativedelta.relativedelta(months=1)
    next_month_date = current_month_date + relativedelta.relativedelta(months=1)

    days_as_dates.extend([previous_month_date.replace(day=day) for day in prev])
    days_as_dates.extend([current_month_date.replace(day=day) for day in cur])
    days_as_dates.extend([next_month_date.replace(day=day) for day in next])

    # flatten df
    df = df.values.flatten()
    # create new Series with dates as index
    series = pd.Series(df, index=pd.to_datetime(days_as_dates))
    # drop Nans
    series = series.dropna()
    for index, value in series.items():
        # 7 корпус 1-7 этажи 7 building 1-7 floors
        # 2 корпус 3-4 этаж 2 building 3-4 floor
        # 3 корпус 3 building
        matches = re.finditer(r"(?P<building>\d)\s+building(\s+(?P<floors>(\d+|\d+-\d+))\s+floors?)?", value)
        for m in matches:
            building = m.group("building")
            floors = m.group("floors")
            if floors:
                key = f"{building} building {floors} floors"
            else:
                key = f"{building} building"
            entries[key].append(index.date())
            logger.debug(f"{key}, {index.date()}")


def parse(dfs: dict[str, pd.DataFrame]) -> dict[str, list[date]]:
    entries: dict[str, list[date]] = defaultdict(list)
    for sheet in dfs:
        df = dfs[sheet]
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        process_dataframe(df, entries)
    return entries


def parse_from_url(url: str) -> dict[str, list[date]]:
    with requests.Session() as session:
        with session.get(url) as response:
            html = response.text
            soup = bs4.BeautifulSoup(html, "html.parser")
            # <div id="sheets-viewport" class="">
            div = soup.find("div", id="sheets-viewport")
            entries: dict[str, list[date]] = defaultdict(list)
            # iterate over children divs
            for child in div.children:
                if isinstance(child, bs4.Tag):
                    table = child.find("table")

                    with io.StringIO() as f:
                        f.write(str(table))
                        f.seek(0)
                        df = pd.read_html(f, flavor="bs4")
                    assert len(df) == 1
                    df = df[0]
                    # drop first column
                    df = df.drop(df.columns[0], axis=1)
                    process_dataframe(df, entries)

            return entries


def get_xlsx_file(spreadsheet_id: str) -> io.BytesIO:
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
    response = httpx.get(export_url, follow_redirects=True)
    logger.debug(f"Response status: {response.status_code}")
    response.raise_for_status()
    # ------- Return xlsx file as BytesIO object -------
    return io.BytesIO(response.content)


if __name__ == "__main__":
    # https://docs.google.com/spreadsheets/d/1xXnyinI1sNQ3ZKTPlKlqJKt4685oCz2R2LzlgEUztKs/export?format=xlsx
    spreadsheet_id = "1xXnyinI1sNQ3ZKTPlKlqJKt4685oCz2R2LzlgEUztKs"

    xlsx_file = get_xlsx_file(spreadsheet_id)
    dfs = pd.read_excel(xlsx_file, sheet_name=None, header=None)
    print(parse(dfs))
