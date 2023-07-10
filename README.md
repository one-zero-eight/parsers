# InNoHassle Parsers

Parsers for InNoHassle ecosystem.

## Description

Contains parsers for InNoHassle ecosystem:

- [schedule](./schedule)
    - [core courses](./schedule/core_courses)
    - [elective courses](./schedule/electives)
    - ~~sport~~ (not implemented yet)

## Usage

### Schedule

1. Install dependencies using [poetry](https://python-poetry.org/):
    ```bash
    poetry install
    ```
2. Run the parser:
    ```bash
    poetry shell
    python -m schedule.core_courses
    python -m schedule.electives
    ```
3. The output will be in the `schedule/output` directory.
