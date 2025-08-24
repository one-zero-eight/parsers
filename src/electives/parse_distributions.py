import json
import warnings
from collections import defaultdict

import pandas as pd

from src.electives.config import electives_config as config
from src.electives.parser import ElectiveParser
from src.utils import sluggify


def normalize_name(name: str) -> str:
    return name.strip().lower().removesuffix(" (advanced)").removesuffix(" [online]")


def main():
    parser = ElectiveParser()
    xlsx = parser.get_xlsx_file(
        spreadsheet_id=config.distribution_spreadsheet_id,
    )
    # ------- Read xlsx file into dataframes -------
    dfs = pd.read_excel(xlsx, engine="openpyxl", sheet_name=None, header=0)
    # ------- Clean up dataframes -------
    dfs = {key.strip(): value for key, value in dfs.items()}

    total_distributions = defaultdict(set)

    for sheet_name, df in dfs.items():
        if sheet_name.lower() == "swap list":
            continue
        email_column = next(col for col in df.columns if col.lower() == "email" or col.lower() == "e-mail")
        elective_columns = [col for col in df.columns if "elective" in col.lower()]
        email_x_elective_names: dict[str, set[str]] = defaultdict(set)

        for elective_column in elective_columns:
            for index, row in df.iterrows():
                email = row[email_column]
                elective = row[elective_column]
                if pd.isna(email) or pd.isna(elective):
                    continue
                email_x_elective_names[email].add(elective)

        elective_name_x_elective_alias: dict[str, str] = {}

        for elective in config.electives:
            elective_name_x_elective_alias[normalize_name(elective.name)] = elective.alias

        DICTIONARY = {
            "системная коммуникация для лидеров и команд (бизнес риторика)": "Conscious Communication. (Business Rhetorics)",
            "публичные выступления для ит специалистов": "Public Speaking for IT-Specialist",
            "киберправо": "Cyberlaw: Data, Ethics and Digital Property",
            "ux/ui": "UX/UI Design",
            "deep dive into system design": "System Design of High-Load Applications",
            "how to build and it team": "How to Build an IT Team",
            "инженерия продуктовых решений": "Product Solutions Engineering",
            "conscious communication (business rhetorics)": "Conscious Communication. (Business Rhetorics)",
        }

        sheet_name_dictionary = {"BS1 RU": "BS1 (рус)", "BS1": "BS1", "BS2": "BS2", "MS": "MS", "MS1": "MS"}

        distributions = defaultdict(set)

        for email, elective_names in email_x_elective_names.items():
            for elective_name in elective_names:
                elective_name = normalize_name(elective_name)
                elective_alias = elective_name_x_elective_alias.get(elective_name)  # TODO: We should match it fuzzy

                if not elective_alias:
                    _elective_name = DICTIONARY.get(elective_name)
                    if _elective_name:
                        elective_alias = elective_name_x_elective_alias.get(normalize_name(_elective_name))

                if not elective_alias:
                    warnings.warn(f"No alias found for `{elective_name}`")
                else:
                    calendar_alias = f"{config.semester_tag.alias}-{sluggify(sheet_name_dictionary[sheet_name])}-{sluggify(elective_alias)}"
                    distributions[email].add(calendar_alias)

        for email, elective_aliases in distributions.items():
            total_distributions[email].update(elective_aliases)

    for email, elective_aliases in total_distributions.items():
        total_distributions[email] = list(elective_aliases)

    with open("electives_distributions.json", "w") as f:
        json.dump(
            {"users": [{"email": email, "groups": groups} for email, groups in total_distributions.items()]},
            f,
            indent=2,
            ensure_ascii=False,
        )


if __name__ == "__main__":
    main()
