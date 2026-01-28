import asyncio
import os
import time
import warnings

from src.cleaning.__main__ import main as cleaning_main
from src.core_courses.__main__ import main as core_courses_main
from src.electives.__main__ import main as electives_main
from src.logging_ import logger
from src.sports.__main__ import main as sports_main


def create_markdown_table_and_details(data_dict, warnings):
    # Initialize the result list to store the markdown content
    result = ["| Category       | Updated | Same |\n", "| -------------- | ------- | ---- |\n"]

    # Iterate over each category (Core Courses, Electives, Sports, etc.)
    for category, courses in data_dict.items():
        updated_count = len(courses["updated"]) if "updated" in courses else "?"
        same_count = len(courses["same"]) if "same" in courses else "?"

        # Add the count of updated and same courses to the table
        result.append(f"| {category} | {updated_count} | {same_count} |\n")

    # Add a newline to separate the table from the foldable sections
    result.append("\n")

    # Add a warning section if there are any warnings
    if warnings:
        result.append("### Warnings ⚠️\n")
        for warning in warnings:
            result.append(f"- {warning.message}\n")
        result.append("\n")

    # Iterate over each category again to create the foldable sections
    for category, courses in data_dict.items():
        result.append(f"### {category}\n")

        # Updated and Same sections for each category
        for section, course_list in courses.items():
            # Create foldable sections for each course code
            result.append(f"<details><summary><b>{section.capitalize()} [{len(course_list)}]</b></summary>\n\n")
            if course_list:
                # as bulleted list
                course_list = [f"- {course}" for course in course_list]
                result.append("\n".join(course_list) + "\n")
            else:
                result.append("Nothing...\n")
            result.append("</details>\n")
            result.append("\n")  # Add space between sections

    # Join the result into a single string
    return "".join(result)


def main():
    result = {}

    logger.info("\nCore Courses:")
    if _ := asyncio.run(core_courses_main()):
        result["Core Courses"] = _

    logger.info("\nElectives:")
    if _ := asyncio.run(electives_main()):
        result["Electives"] = _

    logger.info("\nSports:")
    if _ := asyncio.run(sports_main()):
        result["Sports"] = _

    logger.info("\nCleaning:")
    if _ := cleaning_main():
        result["Cleaning"] = _
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--period", type=int, help="Period in seconds", default=-1)

    args = parser.parse_args()
    logger.info(f"Update schedule of Core Courses, Electives and Sports every {args.period} seconds")

    github_summary = os.environ.get("GITHUB_STEP_SUMMARY")

    if args.period == -1:
        logger.info("Period is -1, run only once")
        with warnings.catch_warnings(record=True) as w:
            output = main()
            if github_summary:
                summary = create_markdown_table_and_details(output, w)
                with open(github_summary, "w") as f:
                    f.write(summary)
            if w:
                logger.warning("Warnings occurred")
                for warning in w:
                    record = logger.makeRecord(
                        logger.name,
                        30,
                        warning.filename,
                        warning.lineno,
                        warning.message,
                        (),
                        None,
                    )
                    logger.handle(record)
                exit(1)
    else:
        while True:
            main()
            logger.info(f"Wait for {args.period} seconds...")
            time.sleep(args.period)
