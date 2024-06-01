from pathlib import Path

from pydantic import BaseModel


class User(BaseModel):
    email: str
    groups: list[str]


class Data(BaseModel):
    __root__: list[User]


def main():
    user_data_file = Path("innopolis_user_data.json")

    with open(user_data_file) as f:
        data = Data.parse_raw(f.read())

    # merge duplicates and drop "" groups
    users = {}
    for user in data.__root__:
        if user.email in users:
            users[user.email].groups.extend(user.groups)
        else:
            users[user.email] = user

    for user in users.values():
        user.groups = list(set(user.groups) - {""})

    data = Data.parse_obj(list(users.values()))

    with open(user_data_file, "w") as f:
        f.write(data.json(indent=2))


if __name__ == '__main__':
    main()
