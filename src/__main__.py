import asyncio
import time

from src.core_courses.__main__ import main as core_courses_main
from src.electives.__main__ import main as electives_main
from src.logging_ import logger
from src.sports.__main__ import main as sports_main


def main():
    logger.info("\nCore Courses:")
    core_courses_main()
    logger.info("\nElectives:")
    electives_main()
    logger.info("\nSports:")
    asyncio.run(sports_main())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--period", type=int, help="Period in seconds", default=600)

    args = parser.parse_args()
    logger.info(f"Update schedule of Core Courses, Electives and Sports every {args.period} seconds")

    while True:
        main()
        logger.info(f"Wait for {args.period} seconds...")
        time.sleep(args.period)
