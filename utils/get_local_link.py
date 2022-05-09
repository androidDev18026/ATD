import csv
import logging
import os
import sys
from typing import List

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s : %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p",
    level="INFO",
)
logger = logging.getLogger()


def get_paths(basedir: str) -> List[str]:
    assert os.path.isdir(basedir), f"No such directory {basedir}"
    assert any(os.scandir(basedir)), f"No files in {basedir}"

    filenames = map(
        lambda f: os.path.abspath(os.path.join(basedir, f)), os.listdir(basedir)
    )

    return sorted(filenames, key=lambda x: int("".join(filter(str.isdigit, x))))


if __name__ == "__main__":

    assert (
        len(sys.argv) == 3
    ), "Not enough arguments!: <raw_articles_dir/> <article_path.csv>"

    paths = get_paths(sys.argv[1])

    logger.info("Found %d articles in %s", len(paths), os.path.abspath(sys.argv[1]))

    logger.info("Reading from %s", sys.argv[1])

    with open(sys.argv[2], mode="w", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(("id", "path"))
        writer.writerows([(i, path) for i, path in enumerate(paths)])
        logger.info("Done writing article paths to %s", sys.argv[2])
