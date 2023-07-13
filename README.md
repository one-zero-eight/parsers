# InNoHassle Parsers


[![MIT License](https://img.shields.io/badge/License-MIT-blue.svg) ](https://opensource.org/licenses/MIT) 
![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat&logo=Python)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Project Description

Schedule parsers for the InNoHassle ecosystem. These parsers parse schedules provided by the university (Google Tables, Sport API, etc.) and convert them into .ics files to make comfortable schedule for the students.
Contains following parsers:

- [schedule](./schedule)
    - [core courses](./schedule/core_courses)
    - [elective courses](./schedule/electives)
    - ~~sport~~ (not implemented yet)

## Demo

Will be later...

## Project Installation

To install the InNoHassle Parsers, follow the steps below:

1. Clone the [repository](https://github.com/one-zero-eight/InNoHassle-Parsers/) repository
2. Install [Python](https://python.org)
3. Install [Poetry](https://python-poetry.org/)
4. Install dependencies using poetry:
    ```bash
    poetry install
    ```

## How to Use

1. Run the parser:

    ```bash
    poetry shell
    python -m schedule.core_courses
    python -m schedule.electives
    ```
2. The output will be in the `schedule/output` directory.

## Features List

- Automatic parsing of schedules provided by the university
- Converting scedules into the .ics files


## Frameworks and Technologies Used

|           Language           |              Frameworks               |              Libraries               |
|:----------------------------:|:-------------------------------------:|:------------------------------------:|
| [Python](https://python.org) | [Poetry](https://python-poetry.org/)  | [pandas](https://pandas.pydata.org/) |
|                              | [Sphinx](https://www.sphinx-doc.org/) | [pydantic](https://pydantic.dev/)    |
## For Customer

We highly value your satisfaction and want to provide you with the best possible support. If you encounter any problems or have any questions regarding our product or service, please don't hesitate to create an **issue**. Our team is here to assist you and will promptly address your concerns. Your feedback is crucial for us to continually improve our offerings, and we appreciate the opportunity to assist you. Thank you for choosing us as your trusted provider, and we assure you of our commitment to your satisfaction.

## License

This project is licensed under the [MIT License](https://license.md/licenses/mit-license/) - see the [LICENSE](LICENSE) file for details.
