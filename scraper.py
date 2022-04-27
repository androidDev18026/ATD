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


def main():

    logging.basicConfig(
        format="[%(levelname)s] %(asctime)s : %(message)s",
        datefmt="%d/%m/%Y %I:%M:%S %p",
    )

    base_urls = (
        "https://www.in.gr/politics/",
        "https://www.zougla.gr/politiki/main",
        "https://www.naftemporiki.gr/politics",
        "https://www.news247.gr/politiki/",
    )

    links = flatten([get_latest_from_url(url) for url in base_urls])

    random.shuffle(links)

    links_with_index = list(zip(range(len(links)), links))

    logger.info("Starting to write links to outfile...")

    with open(sys.argv[1], mode="w", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(("id", "url"))
        writer.writerows(links_with_index)

    logger.info("Done")


if __name__ == "__main__":
    main()
