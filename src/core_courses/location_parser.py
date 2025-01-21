import re
from datetime import date, datetime, time
from functools import partial

from pydantic import BaseModel


class Item(BaseModel):
    location: str | None = None
    starts_from: date | None = None
    starts_at: time | None = None
    till: time | None = None
    on_weeks: list[int] | None = None
    on: list[date] | None = None
    except_: list[date] | None = None
    NEST: list["Item"] | None = None

    class Config:
        arbitrary_types_allowed = True


Item.update_forward_refs()
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
            locations = [l.strip() for l in locations]
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

    _starts_from_pattern = r"\(?(STARTS ON|STARTS FROM|FROM|С)\s*(\d{1,2}[\/.]\d{1,2})\)?"

    def starts_from(y: str):
        if m := re.fullmatch(_starts_from_pattern, y):
            _date = m.group(2).replace(".", "/")
            day, month = _date.split(sep="/")

            return Item(starts_from=ydate(day=int(day), month=int(month)))

    _starts_at_pattern = r"\(?(STARTS|STARTS AT|НАЧАЛО В)\s*(\d{1,2}[:.]\d{1,2})\)?"

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

    _on_pattern = (
        r"\(?(ON|ONLY ON|НА|ТОЛЬКО НА|ТОЛЬКО)\s*(?P<dates>(\d{1,2}[\/.]\d{1,2}(?:,\s*\d{1,2}[\/.]\d{1,2})*))\)?"
    )

    def on(y: str):
        if m := re.fullmatch(_on_pattern, y):
            dates = m.group("dates")
            dates = dates.split(",")
            dates = [d.replace(".", "/") for d in dates]
            dates = [d.split("/") for d in dates]
            dates = [ydate(day=int(d[0]), month=int(d[1])) for d in dates]
            return Item(on=dates)

    _till_pattern = r"\(?TILL\s*(?P<time>\d{1,2}[:.]\d{1,2})\)?"

    def till(y: str):
        if m := re.fullmatch(_till_pattern, y):
            _time = m.group("time").replace(".", ":")
            hour, minute = _time.split(sep=":")
            return Item(till=time(hour=int(hour), minute=int(minute)))

    _except_pattern = r"\(?(EXCEPT|КРОМЕ)\s*(?P<dates_except>(\d{1,2}[\/.]\d{1,2}(?:,\s*\d{1,2}[\/.]\d{1,2})*))\)?"

    def except_(y: str):
        if m := re.fullmatch(_except_pattern, y):
            dates = m.group("dates_except")
            dates = dates.split(",")
            dates = [d.replace(".", "/") for d in dates]
            dates = [d.split("/") for d in dates]
            dates = [ydate(day=int(d[0]), month=int(d[1])) for d in dates]
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
                combined = as_z1.dict(exclude_none=True) | as_z2.dict(exclude_none=True)
                return Item.parse_obj(combined)

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
                combined = as_z1.dict(exclude_none=True) | as_z2.dict(exclude_none=True) | as_z3.dict(exclude_none=True)
                return Item.parse_obj(combined)

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
