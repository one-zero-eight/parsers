"""
This file should be synced between:
https://github.com/one-zero-eight/parsers/blob/main/src/core_courses/location_parser.py
https://github.com/one-zero-eight/schedule-builder-backend/blob/main/src/core_courses/location_parser.py
"""

import re
from datetime import date, datetime, time
from functools import partial

from pydantic import BaseModel, ConfigDict


class Item(BaseModel):
    """
    Represents a parsed location string with optional modifiers and nested locations.
    
    This class is the result of parsing location strings from spreadsheet cells (third row
    in CoreCourseCell). The location string can contain room numbers, online indicators,
    and various temporal modifiers that affect when and where events occur.
    
    ## Flow Overview
    
    ### 1. Input: Spreadsheet Location String
    Location strings come from the third cell value in CoreCourseCell (value[2]).
    Examples:
    - "313" → simple room number
    - "ONLINE" → online event
    - "313 (WEEK 1-3) / ONLINE" → room 313 for weeks 1-3, then online
    - "ONLINE ON 13/09, 108 ON 01/11 (STARTS AT 9:00)" → online on 13/09, room 108 on 01/11, both starting at 9:00
    - "460 EXCEPT 28/11" → room 460, excluding 28/11
    
    ### 2. Parsing: parse_location_string()
    The location string is normalized (uppercased, "AND" replaced with commas) and parsed
    into an Item object. The parser recognizes:
    
    **Locations:**
    - Room numbers: "313", "room 107", "ROOM #107" → location="313" or "107"
    - Online: "ONLINE", "ОНЛАЙН", "ONLINE (TBA)" → location="ONLINE" or "ОНЛАЙН"
    - Unknown: "?" → location="?"
    - Multiple: "106/313/314" → location="106/313/314"
    
    **Modifiers:**
    - `starts_from`: "STARTS FROM 21/09" → starts_from=date(2024, 9, 21)
    - `starts_at`: "STARTS AT 18:00" → starts_at=time(18, 0)
    - `till`: "TILL 21:00" → till=time(21, 0)
    - `on_weeks`: "WEEK 1-3" → on_weeks=[1, 2, 3]
    - `on`: "ON 13/09, 20/09" → on=[date(2024, 9, 13), date(2024, 9, 20)]
    - `except_`: "EXCEPT 30/01, 06/02" → except_=[date(2024, 1, 30), date(2024, 2, 6)]
    
    **Nested Structures (NEST):**
    Complex patterns create nested Items:
    - "313 (WEEK 1-3) / ONLINE" → Item(location="313", on_weeks=[1,2,3], NEST=[Item(location="ONLINE")])
    - "105 ON 15/10, 106 ON 29/10" → Item(location="105", on=[...], NEST=[Item(location="106", on=[...])])
    - "ONLINE ON 13/09, 108 ON 01/11 (STARTS AT 9:00)" → Item with nested items sharing starts_at
        
    ### 3. Output: ICS Calendar Events (generate_vevents())
    The Item is converted to one or more ICS calendar events:
    
    **Simple Case (no location_item):**
    - Creates one recurring event with RRULE (weekly recurrence)
    - Uses event.location, event.start_time, event.end_time
    
    **With location_item:**
    
    **Base Event Properties:**
    - `location`: Uses location_item.location or falls back to event.location
    - `starts`: Uses location_item.starts_from or falls back to event.starts
    - `start_time`: Adjusted if location_item.starts_at exists (keeps duration)
    - `end_time`: Set to location_item.till if exists, otherwise calculated from start_time + duration
    
    **Recurrence Handling:**
    - `on_weeks` → Converted to specific dates using nearest_weekday() + weeks offset, merged into `on`
    - If `on` exists: Creates events with RDATE (specific dates) instead of RRULE
      - Each date in `on` becomes a recurrence date
      - dtstart/dtend adapted to first date in `on`
    - If `on` is None: Creates weekly recurring event with RRULE
    - If `except_` exists: Adds EXDATE to exclude specific dates from recurrence
    
    **Nested Items (NEST):**
    Nested items create additional calendar events:
    
    - If parent has RRULE (weekly recurrence):
      - For each nested item with `on` dates:
        - Creates RECURRENCE-ID events overriding specific recurrence instances
        - Uses nested item's location, starts_at, till if specified
        - Removes RRULE from override event
        - Parent event still yields with RRULE
    
    - If parent has RDATE (specific dates):
      - Creates separate events for nested items
      - Each nested item gets its own RDATE with its `on` dates
      - Uses nested item's location, starts_at, till if specified
      - Parent event yields first, then nested events
    
    **Examples of ICS Output:**
    
    Input: "313"
    → One event: location="313", RRULE=FREQ=WEEKLY
    
    Input: "ONLINE ON 13/09, 20/09"
    → One event: location="ONLINE", RDATE=[2024-09-13, 2024-09-20]
    
    Input: "313 (WEEK 1-3) / ONLINE"
    → Two events:
      1. location="313", RRULE=FREQ=WEEKLY (with EXDATE for weeks after 3)
      2. location="ONLINE", RDATE=[dates for weeks 4+]
    
    Input: "ONLINE ON 13/09, 108 ON 01/11 (STARTS AT 9:00)"
    → Three events:
      1. location="ONLINE", RDATE=[2024-09-13], dtstart=09:00
      2. location="108", RDATE=[2024-11-01], dtstart=09:00
      3. Parent recurring event (if applicable)
    
    Input: "460 EXCEPT 28/11"
    → One event: location="460", RRULE=FREQ=WEEKLY, EXDATE=[2024-11-28]
    
    ## Field Descriptions
    
    :param location: Room number, "ONLINE", "ОНЛАЙН", "?", or slash-separated combinations like "106/313"
    :param starts_from: Date when the event starts (overrides event.starts)
    :param starts_at: Time when the event starts (overrides event.start_time, preserves duration)
    :param till: Time when the event ends (overrides event.end_time)
    :param on_weeks: List of week numbers (1-based, converted to dates during ICS generation)
    :param on: List of specific dates when event occurs (creates RDATE instead of RRULE)
    :param except_: List of dates to exclude from recurrence (creates EXDATE)
    :param NEST: List of nested Item objects for complex location patterns
    """
    location: str | None = None
    starts_from: date | None = None
    starts_at: time | None = None
    till: time | None = None
    on_weeks: list[int] | None = None
    on: list[date] | None = None
    except_: list[date] | None = None
    NEST: list["Item"] | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)


Item.model_rebuild()
ydate = partial(date, year=datetime.today().year)


def parse_location_string(x: str, from_parent: bool = False) -> Item | None:
    x = x.upper()
    x = x.replace("(ONLINE)", r"ONLINE")
    x = x.replace("(ОНЛАЙН)", r"ОНЛАЙН")
    x = x.strip()
    # replace AND with ,
    x = re.sub(r"\s+AND\s+", ", ", x)
    x = re.sub(r"\s+И\s+", ", ", x)

    def combine_patterns(patterns):
        return r"(" + "|".join(patterns) + r")"

    def get_location(y: str):
        if m := re.fullmatch(r"^(\d+)$", y):
            return m.group(1)

        if m := re.fullmatch(r"^\?$", y):
            return "?"

        if m := re.fullmatch(r"^ROOM\s*#?\s*(\d+)$", y):
            return m.group(1)

        if m := re.fullmatch(r"^(ONLINE|ОНЛАЙН)$", y):
            return m.group(0)

        if m := re.fullmatch(r"^(ONLINE|ОНЛАЙН)\s*\(TBA\)$", y):
            return m.group(0)

        if m := re.fullmatch(r"^((\d|ONLINE|ОНЛАЙН)+(?:\s*/\s*(\d|ONLINE|ОНЛАЙН)+)+)$", y):
            locations = m.group(1)
            locations = locations.split("/")
            locations = [location.strip() for location in locations]
            return "/".join(locations)

    _loc = combine_patterns(
        [
            r"(\d+)",
            r"\?",
            r"ROOM\s*#?\s*(\d+)",
            r"(ONLINE|ОНЛАЙН)",
            r"(ONLINE|ОНЛАЙН)\s*\(TBA\)",
            r"((\d|ONLINE|ОНЛАЙН)+(?:\s*/\s*(\d|ONLINE|ОНЛАЙН)+)+)",
        ]
    )

    def location_plus_pattern(group_name: str, pattern: str):
        return rf"(?P<location>{_loc}) \(?(?P<{group_name}>{pattern})\)?"

    if as_simple_location := get_location(y=x):
        return Item(location=as_simple_location)

    _starts_from_pattern = r"\(?(STARTS ON|STARTS FROM|FROM|С|НАЧАЛО С|СТАРТ|СТАРТ С)\s*(\d{1,2}[\/.]\d{1,2})\)?"

    def starts_from(y: str):
        if m := re.fullmatch(_starts_from_pattern, y):
            _date = m.group(2).replace(".", "/")
            day, month = _date.split(sep="/")

            return Item(starts_from=ydate(day=int(day), month=int(month)))

    _starts_at_pattern = r"\(?(STARTS|STARTS AT|НАЧАЛО В|НАЧАЛО)\s*(\d{1,2}[:.]\d{1,2})\)?"

    def starts_at(y: str):
        if m := re.fullmatch(_starts_at_pattern, y):
            _time = m.group(2).replace(".", ":")
            hour, minute = _time.split(sep=":")

            return Item(starts_at=time(hour=int(hour), minute=int(minute)))

    _week_pattern = r"\(?WEEK\s*(?P<weeks>\d+(?:-\d+)?(?:,\s*\d+(?:-\d+)?)*)(?:\s+ONLY)?\)?"

    def week(y: str):
        if m := re.fullmatch(_week_pattern, y):
            weeks = m.group("weeks")
            weeks = weeks.split(",")
            weeks = [w.split("-") for w in weeks]
            weeks = [list(range(int(w[0]), int(w[1]) + 1)) if len(w) == 2 else [int(w[0])] for w in weeks]
            weeks = [item for sublist in weeks for item in sublist]
            return Item(on_weeks=weeks)

    # ON 13/09, 20/09
    # ONLY ON 13/09 20/09
    # ТОЛЬКО НА 13/09, 20/09
    # and etc.
    _date_component_pattern = r"(?P<day>\d{1,2})[\/.](?P<month>\d{1,2})"
    _date_component_non_capturing = r"\d{1,2}[\/.]\d{1,2}"
    _on_pattern = rf"\(?(ON|ONLY ON|НА|ТОЛЬКО НА|ТОЛЬКО)\s*(?P<dates>{_date_component_non_capturing}(?:[,\s]\s*{_date_component_non_capturing})*)\)?"

    def on(y: str):
        if m := re.fullmatch(_on_pattern, y):
            dates_str = m.group("dates")
            dates = [
                ydate(day=int(dm.group("day")), month=int(dm.group("month")))
                for dm in re.finditer(_date_component_pattern, dates_str)
            ]
            return Item(on=dates)

    _till_pattern = r"\(?TILL\s*(?P<time>\d{1,2}[:.]\d{1,2})\)?"

    def till(y: str):
        if m := re.fullmatch(_till_pattern, y):
            _time = m.group("time").replace(".", ":")
            hour, minute = _time.split(sep=":")
            return Item(till=time(hour=int(hour), minute=int(minute)))

    # EXCEPT 30/01 06/02
    # КРОМЕ 30/01, 06/02
    # и т.д.
    _except_pattern = rf"\(?(EXCEPT|КРОМЕ)\s*(?P<dates_except>{_date_component_non_capturing}(?:[,\s]+{_date_component_non_capturing})*)\)?"

    def except_(y: str):
        if m := re.fullmatch(_except_pattern, y):
            dates_str = m.group("dates_except")
            dates = [
                ydate(day=int(dm.group("day")), month=int(dm.group("month")))
                for dm in re.finditer(_date_component_pattern, dates_str)
            ]
            return Item(except_=dates)

    _mod = combine_patterns(
        [_starts_from_pattern, _starts_at_pattern, _week_pattern, _on_pattern, _till_pattern, _except_pattern]
    )

    def any_modifier(y: str):
        if m := re.fullmatch(_mod, y):
            z = m.group(0)
            if as_starts_from := starts_from(z):
                return as_starts_from
            if as_starts_at := starts_at(z):
                return as_starts_at
            if as_week := week(z):
                return as_week
            if as_on := on(z):
                return as_on
            if as_till := till(z):
                return as_till
            if as_except := except_(z):
                return as_except

    if as_any_modifier := any_modifier(x):
        return as_any_modifier

    if m := re.fullmatch(location_plus_pattern("any_modifier", _mod), x):
        location = get_location(m.group("location"))
        as_any_modifier = any_modifier(m.group("any_modifier"))
        as_any_modifier.location = location
        return as_any_modifier

    # replace all named groups with non-capturing groups
    _mod_noname = re.sub(r"\(\?P<[^>]+>", "(?:", _mod)
    _two_modifiers_pattern = rf"\(?(?P<first>{_mod_noname})\)?\s*\(?(?P<second>{_mod_noname})\)?"

    def two_modifiers(y: str):
        if m := re.fullmatch(_two_modifiers_pattern, y):
            z1, z2 = m.group("first"), m.group("second")
            as_z1 = any_modifier(z1)
            as_z2 = any_modifier(z2)
            if as_z1 and as_z2:
                combined = as_z1.model_dump(exclude_none=True) | as_z2.model_dump(exclude_none=True)
                return Item.model_validate(combined)

    if as_two_modifiers := two_modifiers(x):
        return as_two_modifiers

    if m := re.fullmatch(location_plus_pattern("two_modifiers", _two_modifiers_pattern), x):
        location = get_location(m.group("location"))
        as_two_modifiers = two_modifiers(m.group("two_modifiers"))
        as_two_modifiers.location = location
        return as_two_modifiers
    _three_modifiers_pattern = (
        rf"\(?(?P<first>{_mod_noname})\)?\s*\(?(?P<second>{_mod_noname})\)?\s*\(?(?P<third>{_mod_noname})\)?"
    )

    def three_modifiers(y: str):
        if m := re.fullmatch(_three_modifiers_pattern, y):
            z1, z2, z3 = m.group("first"), m.group("second"), m.group("third")
            as_z1 = any_modifier(z1)
            as_z2 = any_modifier(z2)
            as_z3 = any_modifier(z3)
            if as_z1 and as_z2 and as_z3:
                combined = (
                    as_z1.model_dump(exclude_none=True)
                    | as_z2.model_dump(exclude_none=True)
                    | as_z3.model_dump(exclude_none=True)
                )
                return Item.model_validate(combined)

    if as_three_modifiers := three_modifiers(x):
        return as_three_modifiers

    if m := re.fullmatch(location_plus_pattern("three_modifiers", _three_modifiers_pattern), x):
        location = get_location(m.group("location"))
        as_three_modifiers = three_modifiers(m.group("three_modifiers"))
        as_three_modifiers.location = location
        return as_three_modifiers

    if from_parent:  # only one nesting level
        return None

    _simple_nest_pattern = rf"(?P<location>{_loc})\s*\(?(?P<rest>.+)\)?"

    def simple_nest(y: str):
        if m := re.fullmatch(_simple_nest_pattern, y):
            location = get_location(m.group("location"))
            rest = parse_location_string(m.group("rest"), from_parent=True)
            if rest is not None:
                return Item(location=location, NEST=[rest])

    if as_simple_nest := simple_nest(x):
        return as_simple_nest

    # 313 (WEEK 1-3) / ONLINE
    __1 = rf"(?P<location>{_loc})\s*\(?(?P<modifier>{_mod_noname})\)?\s*/\s*(?P<another>.+)"

    def _1(y: str):
        if m := re.fullmatch(__1, y):
            location = get_location(m.group("location"))
            modifier = any_modifier(m.group("modifier"))
            another = parse_location_string(m.group("another"), from_parent=True)
            if modifier and another:
                modifier.location = location
                modifier.NEST = [another]
                return modifier

    if as__1 := _1(x):
        return as__1

    # 105 ON 15/10, 106 ON 29/10, ONLINE ON 05/11
    __4_3 = rf"(?P<l1>{_loc})\s*(?P<m1>{_mod_noname})\s*,\s*(?P<l2>{_loc})\s*(?P<m2>{_mod_noname})\s*,\s*(?P<l3>{_loc})\s*(?P<m3>{_mod})"
    # 105 ON 15/10, 106 ON 29/10
    __4_2 = rf"(?P<l1>{_loc})\s*(?P<m1>{_mod_noname})\s*,\s*(?P<l2>{_loc})\s*(?P<m2>{_mod_noname})"

    def _4(y: str):
        if m := re.fullmatch(__4_2, y):
            l1 = get_location(m.group("l1"))
            m1 = any_modifier(m.group("m1"))
            l2 = get_location(m.group("l2"))
            m2 = any_modifier(m.group("m2"))
            if m1 and m2:
                m1.location = l1
                m2.location = l2
                m1.NEST = [m2]
                return m1
        if m := re.fullmatch(__4_3, y):
            l1 = get_location(m.group("l1"))
            m1 = any_modifier(m.group("m1"))
            l2 = get_location(m.group("l2"))
            m2 = any_modifier(m.group("m2"))
            l3 = get_location(m.group("l3"))
            m3 = any_modifier(m.group("m3"))
            if m1 and m2 and m3:
                m1.location = l1
                m2.location = l2
                m3.location = l3
                m1.NEST = [m2, m3]
                return m1

    if as__4 := _4(x):
        return as__4

    # ONLINE ON 13/09, 108 ON 01/11 (STARTS AT 9:00)
    __2 = rf"(?P<location>{_loc})\s*\(?(?P<modifier>{_mod_noname})\)?\s*,\s*(?P<another>.+?)\s*\(?(?P<common_modifier>{_mod_noname})\)?"

    def _2(y: str):
        if m := re.fullmatch(__2, y):
            location = get_location(m.group("location"))
            modifier = any_modifier(m.group("modifier"))
            another = parse_location_string(m.group("another"), from_parent=True)
            common_modifier = any_modifier(m.group("common_modifier"))

            if modifier and another and common_modifier:
                common_modifier.location = location
                if common_modifier.starts_at:
                    another.starts_at = common_modifier.starts_at
                if common_modifier.till:
                    another.till = common_modifier.till
                common_modifier.on = modifier.on
                common_modifier.NEST = [another]
                return common_modifier

    if as__2 := _2(x):
        return as__2

    # 314 (312 ON 12/09,19/09,26/09) 301 ON 03/10
    __3 = rf"(?P<location>{_loc})\s*\(?(?P<location2>{_loc})\s*(?P<modifier>{_mod_noname})\)?\s*(?P<another>.+)"

    def _3(y: str):
        if m := re.fullmatch(__3, y):
            location = get_location(m.group("location"))
            location2 = get_location(m.group("location2"))
            modifier = any_modifier(m.group("modifier"))
            another = parse_location_string(m.group("another"), from_parent=True)
            if modifier and another:
                modifier.location = location2
                item = Item(location=location, NEST=[modifier, another])
                return item

    if as__3 := _3(x):
        return as__3

    # 107 (106 НА 16.09, 105 НА 07.10) = loc (loc1 mod1, loc2 mod2)
    __5 = rf"(?P<loc>{_loc})\s*\((?P<loc1>{_loc})\s*(?P<mod1>{_mod_noname}),\s*(?P<loc2>{_loc})\s*(?P<mod2>{_mod_noname})\)"

    def _5(y: str):
        if m := re.fullmatch(__5, y):
            loc = get_location(m.group("loc"))
            loc1 = get_location(m.group("loc1"))
            mod1 = any_modifier(m.group("mod1"))
            loc2 = get_location(m.group("loc2"))
            mod2 = any_modifier(m.group("mod2"))
            if mod1 and mod2:
                mod1.location = loc1
                mod2.location = loc2
                return Item(location=loc, NEST=[mod1, mod2])

    if as__5 := _5(x):
        return as__5

    # 317 ON 15/02, 22/02, 15/03, 22/03, 5/04, 12/04, 19/04 (ONLINE ON 26/04)
    __6 = rf"(?P<location>{_loc})\s*(?P<modifier>{_mod_noname})\s*\((?P<location2>{_loc})\s*(?P<mod2>{_mod_noname})\)"

    def _6(y: str):
        if m := re.fullmatch(__6, y):
            loc = get_location(m.group("location"))
            mod = any_modifier(m.group("modifier"))
            loc2 = get_location(m.group("location2"))
            mod2 = any_modifier(m.group("mod2"))
            if mod and mod2:
                mod.location = loc
                mod2.location = loc2
                mod.NEST = [mod2]
                return mod

    if as__6 := _6(x):
        return as__6

    return None
