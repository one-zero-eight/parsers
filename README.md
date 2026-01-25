# Parsers | InNoHassle ecosystem

## Table of contents

Did you know that GitHub supports table of
contents [by default](https://github.blog/changelog/2021-04-13-table-of-contents-support-in-markdown-files/) 🤔

## About

Schedule parsers for the InNoHassle ecosystem. These parsers parse schedules provided by the university
(Google Tables, Sport API, etc.) and updates [InNoHassle Events](https://github.com/one-zero-eight/events) with it.

### Features

Parse schedules:

- [core courses](src/core_courses) from Google Spreadsheet
- [elective courses](src/electives) from another Google Spreadheet
- [sports](src/sports) from [InnoSport](https://sport.innopolis.university/)
- [cleaning](src/cleaning) semi-automatic based
  on [tables](https://hotel.innopolis.university/studentaccommodation/)
- [bootcamp](src/bootcamp) and [workshops](src/workshops) semi-automatic

### Technologies

- [Python 3.12](https://www.python.org/downloads/release/python-3123/) & [uv](https://docs.astral.sh/uv/)
- [Pydantic 2](https://docs.pydantic.dev/latest/)
- [Pandas](https://pandas.pydata.org/)
- [Google API client](https://github.com/googleapis/google-api-python-client)
- Formatting and linting: [Ruff](https://docs.astral.sh/ruff/)

## Development

### Getting started

1. Install [Python 3.12](https://www.python.org/downloads/)
2. Install [uv](https://docs.astral.sh/uv/)
3. Install project dependencies with [uv](https://docs.astral.sh/uv/).
   ```bash
   uv sync
   ```

## How to Use

1. Configure parsers using `config.yaml` files
2. Run the parser:
    ```bash
    uv run -m src.core_courses
    ```
    OR for periodic update of all schedules
    ```bash
    uv run -m src.core_courses --period 600
    ```
3. The output will be in the `schedule/output` directory.

## Contributing

We are open to contributions of any kind.
You can help us with code, bugs, design, documentation, media, new ideas, etc.
If you are interested in contributing, please read
our [contribution guide](https://github.com/one-zero-eight/.github/blob/main/CONTRIBUTING.md).
