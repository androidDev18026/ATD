import os
import sys
import requests
import re
import random
import csv
import logging

from bs4 import BeautifulSoup
from typing import List
from extract import VALID_SITES


ses = requests.session()

logger = logging.getLogger()
logger.setLevel("INFO")


def get_in_gr(soup: BeautifulSoup) -> List[str]:
    links = soup.find_all("a", {"class": "tile relative-title"})
    return [i["href"] for i in links]


def get_zougla(soup: BeautifulSoup) -> List[str]:
    links = soup.find_all("div", {"class": "secondary_story_content"})
    return [
        "https://www.zougla.gr/politiki/" + i.find("a", href=True)["href"]
        for i in links
    ]


def get_naftemporiki(soup: BeautifulSoup) -> List[str]:
    links = soup.find_all("h4")
    prepend = "https://www.naftemporiki.gr"
    return [
        prepend + i.find("a", href=True)["href"]
        for i in links
        if i.find("a", href=True)["href"].startswith("/story")
    ]


def get_news247(soup: BeautifulSoup) -> List[str]:
    links = soup.find_all("h3", {"class": "article__title bold"})
    return [i.find("a", href=True)["href"] for i in links]


def get_latest_from_url(url):
    ses.cookies.clear()
    res = ses.get(url, timeout=2, headers={"Content-Type": "text/html; charset=UTF-8"})

    if res.ok:
        soup = BeautifulSoup(res.text, "html.parser")
        base_url = re.split(r"\b(?:(/)(?!\1))+\b", url)[0]

        if base_url in VALID_SITES:
            if base_url == VALID_SITES[0]:
                res = get_in_gr(soup)
                logger.info(f"Found {len(res)} articles from {base_url}")

            elif base_url == VALID_SITES[1]:
                res = get_zougla(soup)
                logger.info(f"Found {len(res)} articles from {base_url}")
            elif base_url == VALID_SITES[2]:
                res = get_naftemporiki(soup)
                logger.info(f"Found {len(res)} articles from {base_url}")
            elif base_url == VALID_SITES[3]:
                res = get_news247(soup)
                logger.info(f"Found {len(res)} articles from {base_url}")

            return res
        raise ValueError("Provided URL is invalid")


def flatten(t) -> List:
    return [item for sublist in t for item in sublist]


def file_exists(filepath: str):
    if os.path.isfile(filepath) and os.stat(filepath, follow_symlinks=False).st_size:
        logger.info("Found non-empty file %s", os.path.abspath(filepath))
        return True

    logger.warning("File %s not found", filepath)
    return False


def get_num_lines(filepath: str) -> int:
    with open(filepath) as f:
        count = sum(1 for _ in f)

    return count - 1


def links_to_file(outfile: str, links: List[str], override: bool = False):

    fmode, start_idx = "w", 0

    if file_exists(outfile) and not override:
        logger.info("Appending to already existing file %s", outfile)
        fmode = "a"
        start_idx = get_num_lines(outfile)

    with open(outfile, mode=fmode, encoding="utf-8") as out:
        writer = csv.writer(out)

        if fmode == "w":
            logger.info("Writing to new file %s", outfile)
            writer.writerow(("id", "url"))
            writer.writerows(list(zip(range(len(links)), links)))
        else:
            writer.writerows(list(zip(range(start_idx, start_idx + len(links)), links)))


def main():

    logging.basicConfig(
        format="[%(levelname)s] %(asctime)s : %(message)s",
        datefmt="%d/%m/%Y %I:%M:%S %p",
    )

    assert len(sys.argv) == 3, "Not enough arguments: <outfile.csv> <0/1>"

    base_urls = (
        "https://www.in.gr/politics/",
        "https://www.zougla.gr/politiki/main",
        "https://www.naftemporiki.gr/politics",
        "https://www.news247.gr/politiki/",
    )

    links = flatten([get_latest_from_url(url) for url in base_urls])

    # random.shuffle(links)

    if int(sys.argv[2]) == 1:
        logger.warning(
            "File already exists but user requested to discard %s", sys.argv[1]
        )
        links_to_file(sys.argv[1], links, override=True)
    else:
        links_to_file(sys.argv[1], links)

    logger.info("Done")


if __name__ == "__main__":
    main()
