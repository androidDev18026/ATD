import csv
import glob
import logging
import os
import re
import sys
from datetime import datetime
from unicodedata import normalize
from xml.etree.ElementTree import ParseError

import pandas as pd
from bs4 import BeautifulSoup

from crawler import PoliticsCrawler as crawler

VALID_SITES = (
    "https://www.in.gr",
    "https://www.zougla.gr",
    "https://www.naftemporiki.gr",
    "https://www.news247.gr",
)


class DirectoryNotFound(FileNotFoundError):
    __module__ = FileNotFoundError.__module__


class Selector:
    IN = [".main-content > div:nth-child(2)", ".floated-content > div:nth-child(1)"]
    ZOUGLA = "div.article-container:nth-child(2) > div:nth-child(1) > div:nth-child(8)"
    NAFTEMPORIKI = "#leftPHArea_Div1 > div:nth-child(1)"
    NEWS247 = ["div", {"class": "article-body__body"}]


class Extractor:
    def __init__(self, dirname: str, links: str):
        self.dirname = Extractor.validate_directory(dirname)
        self.links = links
        self.html_raw = []
        self.titles = []
        self.bodies = []
        self.csv_out: pd.DataFrame = None

    def __repr__(self) -> str:
        return f"Reading from {self.dirname}"

    def find_all_files(self):

        self.html_raw = sorted(
            glob.glob(f"{self.dirname}/*.html", recursive=False),
            key=lambda x: int("".join(filter(str.isdigit, x))),
        )

    def map_to_links(self, simple=True):

        urls = crawler.get_all_links(self.links)
        urls = [re.split(r"\b(?:(/)(?!\1))+\b", s)[0] for s in urls] if simple else urls

        return dict(zip(self.html_raw, urls))

    def get_selector(self, doc):

        map_ = self.map_to_links()
        selector = None

        if map_[doc] in VALID_SITES:
            site = map_[doc]

            if site == VALID_SITES[0]:
                selector = Selector.IN
            elif site == VALID_SITES[1]:
                selector = Selector.ZOUGLA
            elif site == VALID_SITES[2]:
                selector = Selector.NAFTEMPORIKI
            elif site == VALID_SITES[3]:
                selector = Selector.NEWS247

        return selector if selector is not None else None

    @staticmethod
    def get_soup(html_doc: str, parser="html.parser") -> BeautifulSoup:
        with open(html_doc, "r", encoding="utf-8") as infile:
            raw = infile.read()

        soup = BeautifulSoup(raw, parser)

        if soup:
            return soup
        else:
            raise ParseError("Cannot parse documents.")

    def extract_main(self, html_doc):

        soup = self.get_soup(html_doc)
        selector = self.get_selector(html_doc)

        if isinstance(selector, list) and not (
            any(filter(lambda x: x.__len__() < 3, selector))
        ):
            if soup.select(selector[0], limit=1):
                article_body = "".join(
                    soup.select(selector[0], limit=1)[0].find_all(string=True)
                )
            else:
                article_body = "".join(
                    soup.select(selector[1], limit=1)[0].find_all(string=True)
                )

        elif isinstance(selector, str):
            if soup.select(selector, limit=1):
                article_body = "".join(
                    soup.select(selector, limit=1)[0].find_all(string=True)
                )
            else:
                # Naftermporiki loophole
                article_body = "".join(
                    map(
                        lambda x: normalize("NFKD", x.get_text(strip=True)),
                        soup.select("#spBody", limit=1)[0].find_all("p", string=True),
                    )
                )

        else:
            article_body_tags = soup.find(*selector).find_all("p", recursive=False)[:-1]
            article_body = "".join([t.text for t in article_body_tags])

        clean_article_body = re.sub(r"[^\w .~;]+", "", article_body).strip()

        return clean_article_body

    def get_title(self, html_doc, default="Empty"):

        soup = self.get_soup(html_doc)

        title = soup.title.text

        title = re.sub(r"\W", " ", title)
        title = re.sub(r"\s\s+", " ", title)

        return title.strip() if title else default

    def get_all_bodies(self):
        self.bodies = [self.extract_main(doc) for doc in self.html_raw if self.html_raw]

    def get_all_titles(self):
        self.titles = [self.get_title(doc) for doc in self.html_raw if self.html_raw]

    def construct_csv(self):
        df_tmp = []

        map_ = self.map_to_links(simple=False)

        self.get_all_bodies()
        self.get_all_titles()

        for i, doc_path in enumerate(self.html_raw):
            body = self.bodies[i]
            title = self.titles[i]
            now = datetime.isoformat(datetime.now(), sep=" ", timespec="seconds")

            df_tmp += [
                (
                    title,
                    body,
                    len(body),
                    len(body.encode("utf-8")),
                    map_[doc_path],
                    now,
                )
            ]

        self.csv_out = pd.DataFrame(
            df_tmp,
            columns=("title", "body", "length", "size_bytes", "url", "wall_time"),
        )

    @staticmethod
    def validate_directory(dir):
        if os.path.isdir(os.path.abspath(dir)):
            return os.path.abspath(dir)
        raise DirectoryNotFound(f"{dir}/")


def main():

    logging.basicConfig(
        format="[%(levelname)s] %(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p"
    )

    logger = logging.getLogger()
    logger.setLevel("INFO")

    logger.info("Starting the extractor...")

    assert (
        len(sys.argv) == 4
    ), "Not enough arguments: <html dir> <links.csv> <outfile.csv>"

    extractor = Extractor(sys.argv[1], sys.argv[2])

    extractor.find_all_files()
    extractor.construct_csv()

    # Write csv to outfile
    try:
        extractor.csv_out.to_csv(
            sys.argv[3],
            sep=",",
            header=True,
            encoding="utf-8",
            index_label="id",
            quoting=csv.QUOTE_NONE,
        )
        logger.info("Done writing to %s", sys.argv[3])
    except IOError:
        logger.error("Failed to write to %s", sys.argv[3])


if __name__ == "__main__":
    main()
