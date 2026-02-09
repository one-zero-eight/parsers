from datetime import date, datetime, time
from functools import partial
from unittest import TestCase

import pytest

from src.core_courses.location_parser import Item, parse_location_string

ydate = partial(date, year=datetime.today().year)

cases = [
    # Simple
    ("303", Item(location="303")),
    ("room 107", Item(location="107")),
    ("room #107", Item(location="107")),
    ("ROOM #107", Item(location="107")),
    ("ONLINE", Item(location="ONLINE")),
    ("ОНЛАЙН", Item(location="ОНЛАЙН")),
    ("online", Item(location="ONLINE")),
    ("106/313/314/316/318/320/421", Item(location="106/313/314/316/318/320/421")),
    ("105/ (ONLINE)", Item(location="105/ONLINE")),
    # starts_from modifier
    ("STARTS ON 2/10", Item(starts_from=ydate(day=2, month=10))),
    ("STARTS FROM 21/09", Item(starts_from=ydate(day=21, month=9))),
    ("304 Starts from 19/09", Item(location="304", starts_from=ydate(day=19, month=9))),
    ("313 (STARTS FROM 21/09)", Item(location="313", starts_from=ydate(day=21, month=9))),
    # starts_at modifier
    ("STARTS AT 16.10", Item(starts_at=time(hour=16, minute=10))),
    ("107 STARTS AT 16.10", Item(location="107", starts_at=time(hour=16, minute=10))),
    ("107 (STARTS AT 10.50)", Item(location="107", starts_at=time(hour=10, minute=50))),
    # week modifiers
    ("WEEK 2-4 ONLY", Item(on_weeks=[2, 3, 4])),
    ("105 (WEEK 2-3 ONLY)", Item(location="105", on_weeks=[2, 3])),
    ("105 (WEEK 2, 4 ONLY)", Item(location="105", on_weeks=[2, 4])),
    ("105 (WEEK 2 ONLY)", Item(location="105", on_weeks=[2])),
    ("105 (WEEK 2)", Item(location="105", on_weeks=[2])),
    # on modifier
    ("ON 13/09", Item(on=[ydate(day=13, month=9)])),
    ("ONLY ON 13/09", Item(on=[ydate(day=13, month=9)])),
    ("ТОЛЬКО НА 13/09", Item(on=[ydate(day=13, month=9)])),
    ("НА 13/09", Item(on=[ydate(day=13, month=9)])),
    ("ONLY ON 13/09, 20/09", Item(on=[ydate(day=13, month=9), ydate(day=20, month=9)])),
    ("НА 13/09, 20/09", Item(on=[ydate(day=13, month=9), ydate(day=20, month=9)])),
    ("ON 22/01 29/01", Item(on=[ydate(day=22, month=1), ydate(day=29, month=1)])),
    ("ON 13.09 20.09", Item(on=[ydate(day=13, month=9), ydate(day=20, month=9)])),
    ("ON 13/09  20/09", Item(on=[ydate(day=13, month=9), ydate(day=20, month=9)])),
    ("ONLINE ON 13/09", Item(location="ONLINE", on=[ydate(day=13, month=9)])),
    ("(ONLY ON 10/10)", Item(on=[ydate(day=10, month=10)])),
    (
        "107 (ONLY ON 8/09, 29/09, 27/10, 17/11)",
        Item(
            location="107",
            on=[ydate(day=8, month=9), ydate(day=29, month=9), ydate(day=27, month=10), ydate(day=17, month=11)],
        ),
    ),
    (
        "107 (ON 8/09, 29/09, 27/10, 17/11)",
        Item(
            location="107",
            on=[ydate(day=8, month=9), ydate(day=29, month=9), ydate(day=27, month=10), ydate(day=17, month=11)],
        ),
    ),
    (
        "ONLINE (only on 31/08 and 14/09)",
        Item(location="ONLINE", on=[ydate(day=31, month=8), ydate(day=14, month=9)]),
    ),
    # till modifier
    ("TILL 18:00", Item(till=time(hour=18, minute=0))),
    ("107 (TILL 18:00)", Item(location="107", till=time(hour=18, minute=0))),
    # Multiple modifiers
    (
        "STARTS AT 18:00 TILL 21:00",
        Item(starts_at=time(hour=18, minute=0), till=time(hour=21, minute=0)),
    ),
    (
        "TILL 21:00 STARTS AT 18:00",
        Item(starts_at=time(hour=18, minute=0), till=time(hour=21, minute=0)),
    ),
    (
        "(STARTS AT 18:00) TILL 21:00",
        Item(starts_at=time(hour=18, minute=0), till=time(hour=21, minute=0)),
    ),
    (
        "ON 13/09 STARTS AT 18:00",
        Item(on=[ydate(day=13, month=9)], starts_at=time(hour=18, minute=0)),
    ),
    (
        "ONLINE ON 13/09 STARTS AT 18:00",
        Item(location="ONLINE", on=[ydate(day=13, month=9)], starts_at=time(hour=18, minute=0)),
    ),
    (
        "107 (TILL 21:00) STARTS AT 18:00",
        Item(location="107", starts_at=time(hour=18, minute=0), till=time(hour=21, minute=0)),
    ),
    # NEST
    ("317 (421 ON 11/10)", Item(location="317", NEST=[Item(location="421", on=[ydate(day=11, month=10)])])),
    (
        "111 (106 ON 22/01 29/01)",
        Item(location="111", NEST=[Item(location="106", on=[ydate(day=22, month=1), ydate(day=29, month=1)])]),
    ),
    (
        "105 (room #107 on 28/08)",
        Item(location="105", NEST=[Item(location="107", on=[ydate(day=28, month=8)])]),
    ),
    (
        "313 (WEEK 1-3) / ONLINE",
        Item(location="313", on_weeks=[1, 2, 3], NEST=[Item(location="ONLINE")]),
    ),
    (
        "ONLINE ON 13/09, 108 ON 01/11 (STARTS AT 9:00)",
        Item(
            location="ONLINE",
            on=[ydate(day=13, month=9)],
            starts_at=time(hour=9, minute=0),
            NEST=[
                Item(location="108", on=[ydate(day=1, month=11)], starts_at=time(hour=9, minute=0)),
            ],
        ),
    ),
    (
        "314 (312 ON 12/09,19/09,26/09) 301 ON 03/10",
        Item(
            location="314",
            NEST=[
                Item(
                    location="312",
                    on=[
                        ydate(day=12, month=9),
                        ydate(day=19, month=9),
                        ydate(day=26, month=9),
                    ],
                ),
                Item(location="301", on=[ydate(day=3, month=10)]),
            ],
        ),
    ),
    (
        "107 (STARTS at 18:00) TILL 21:00",
        Item(location="107", starts_at=time(hour=18, minute=0), till=time(hour=21, minute=0)),
    ),
    (
        "105 ON 15/10, 106 ON 29/10, ONLINE ON 05/11",
        Item(
            location="105",
            on=[ydate(day=15, month=10)],
            NEST=[
                Item(location="106", on=[ydate(day=29, month=10)]),
                Item(location="ONLINE", on=[ydate(day=5, month=11)]),
            ],
        ),
    ),
    (
        "107 (106 НА 16.09, 105 НА 07.10)",
        Item(
            location="107",
            NEST=[Item(location="106", on=[ydate(day=16, month=9)]), Item(location="105", on=[ydate(day=7, month=10)])],
        ),
    ),
    (
        "313 (105 ON 18/09, 09/10, 23/10, 30/10)",
        Item(
            location="313",
            NEST=[
                Item(
                    location="105",
                    on=[
                        ydate(day=18, month=9),
                        ydate(day=9, month=10),
                        ydate(day=23, month=10),
                        ydate(day=30, month=10),
                    ],
                ),
            ],
        ),
    ),
    (
        "ONLINE ON 11/09, 313 ON 30/10",
        Item(location="ONLINE", on=[ydate(day=11, month=9)], NEST=[Item(location="313", on=[ydate(day=30, month=10)])]),
    ),
    (
        "301 (ON 15/10, 29/10, 05/11)",
        Item(location="301", on=[ydate(day=15, month=10), ydate(day=29, month=10), ydate(day=5, month=11)]),
    ),
    ("ОНЛАЙН (С 25.09)", Item(location="ОНЛАЙН", starts_from=ydate(day=25, month=9))),
    (
        "ОНЛАЙН (ТОЛЬКО 04/10, 18/10, 01/11, 15/11)",
        Item(
            location="ОНЛАЙН",
            on=[ydate(day=4, month=10), ydate(day=18, month=10), ydate(day=1, month=11), ydate(day=15, month=11)],
        ),
    ),
    (
        "ОНЛАЙН (ТОЛЬКО 27/09, 11/10, 25/10, 08/11, 22/11, 06/12, 20/12) НАЧАЛО В 18:00",
        Item(
            location="ОНЛАЙН",
            on=[
                ydate(day=27, month=9),
                ydate(day=11, month=10),
                ydate(day=25, month=10),
                ydate(day=8, month=11),
                ydate(day=22, month=11),
                ydate(day=6, month=12),
                ydate(day=20, month=12),
            ],
            starts_at=time(hour=18, minute=0),
        ),
    ),
    (
        "314 (? ON 01/10)",
        Item(location="314", NEST=[Item(location="?", on=[ydate(day=1, month=10)])]),
    ),
    (
        "421 (316 FROM 31/10)",
        Item(location="421", NEST=[Item(location="316", starts_from=ydate(day=31, month=10))]),
    ),
    ("460 EXCEPT 28/11", Item(location="460", except_=[ydate(day=28, month=11)])),
    ("303 КРОМЕ 18/11", Item(location="303", except_=[ydate(day=18, month=11)])),
    ("314 EXCEPT 30/01 06/02", Item(location="314", except_=[ydate(day=30, month=1), ydate(day=6, month=2)])),
    ("460 EXCEPT 28.11 15.12", Item(location="460", except_=[ydate(day=28, month=11), ydate(day=15, month=12)])),
    ("303 КРОМЕ 18/11, 25/11", Item(location="303", except_=[ydate(day=18, month=11), ydate(day=25, month=11)])),
    (
        "317 ON 15/02, 22/02, 15/03, 22/03, 5/04, 12/04, 19/04 (ONLINE ON 26/04)",
        Item(
            location="317",
            on=[
                ydate(day=15, month=2),
                ydate(day=22, month=2),
                ydate(day=15, month=3),
                ydate(day=22, month=3),
                ydate(day=5, month=4),
                ydate(day=12, month=4),
                ydate(day=19, month=4),
            ],
            NEST=[Item(location="ONLINE", on=[ydate(day=26, month=4)])],
        ),
    ),
    ("ОНЛАЙН (TBA) НАЧАЛО В 18:30", Item(location="ОНЛАЙН (TBA)", starts_at=time(hour=18, minute=30))),
    (
        "ONLINE ON 10/02, 17/02, 03/03, 10/03 STARTS AT 11:00 TILL 14:00",
        Item(
            location="ONLINE",
            on=[ydate(day=10, month=2), ydate(day=17, month=2), ydate(day=3, month=3), ydate(day=10, month=3)],
            starts_at=time(hour=11, minute=0),
            till=time(hour=14, minute=0),
        ),
    ),
    (
        "460 STARTS FROM 07/04 (ONLINE ON 07/04)",
        Item(
            location="460",
            starts_from=ydate(day=7, month=4),
            NEST=[Item(location="ONLINE", on=[ydate(day=7, month=4)])],
        ),
    ),
    (
        "ONLINE FROM 13/02 STARTS AT 10:00 TILL 11:30",
        Item(
            location="ONLINE",
            starts_from=ydate(day=13, month=2),
            starts_at=time(hour=10, minute=0),
            till=time(hour=11, minute=30),
        ),
    ),
    # Test cases from Item class docstring examples
    # From "Examples of ICS Output" section
    (
        "ONLINE ON 13/09, 20/09",
        Item(location="ONLINE", on=[ydate(day=13, month=9), ydate(day=20, month=9)]),
    ),
    # From "Locations" section - ensure all examples are covered
    ("?", Item(location="?")),
    ("ONLINE (TBA)", Item(location="ONLINE (TBA)")),
    ("106/313/314", Item(location="106/313/314")),
    # From "Modifiers" section - ensure exact examples match
    ("STARTS AT 18:00", Item(starts_at=time(hour=18, minute=0))),
    ("EXCEPT 30/01, 06/02", Item(except_=[ydate(day=30, month=1), ydate(day=6, month=2)])),
]


@pytest.mark.parametrize("input_, desired", cases, ids=[x for x, _ in cases])
def test_location_parser(input_: str, desired: Item):
    result = parse_location_string(input_)
    assert result is not None, f"Failed to parse location string: {input_}"
    _ = TestCase()
    _.maxDiff = None
    _.assertDictEqual(result.model_dump(exclude_none=True), desired.model_dump(exclude_none=True))
