import os
import glob
import re

from bs4 import BeautifulSoup
from crawler import PoliticsCrawler as crawler

VALID_SITES = ("https://www.in.gr", "https://www.zougla.gr", "https://www.naftemporiki.gr")


class DirectoryNotFound(FileNotFoundError):
    __module__ = FileNotFoundError.__module__


class CSSSelector:
    IN = ".main-content > div:nth-child(2)"
    ZOUGLA = "div.article-container:nth-child(2) > div:nth-child(1) > div:nth-child(8)"
    NAFTEMPORIKI = "#leftPHArea_Div1 > div:nth-child(1)"


class Extractor:
    def __init__(self, dirname: str, links: str):
        self.dirname = Extractor.validate_directory(dirname)
        self.links = links
        self.html_raw = []

    def __repr__(self) -> str:
        return f"Reading from {self.dirname}"

    def find_all_files(self):
        self.html_raw = sorted(
            glob.glob(f"{self.dirname}/*.html", recursive=False),
            key=lambda x: int("".join(filter(str.isdigit, x))),
        )

    def map_to_links(self):

        urls = crawler.get_all_links(self.links)
        urls = [re.split(r"\b(?:(/)(?!\1))+\b", s)[0] for s in urls]
      
        return dict(zip(self.html_raw, urls))

    def get_selector(self, doc):

        map_ = self.map_to_links()
        selector = None

        if map_[doc] in VALID_SITES:
            site = map_[doc]

            if site == VALID_SITES[0]:
                selector = CSSSelector.IN
            elif site == VALID_SITES[1]:
                selector = CSSSelector.ZOUGLA
            elif site == VALID_SITES[2]:
                selector = CSSSelector.NAFTEMPORIKI

        return selector if selector is not None else None

    def extract_main(self, html_doc):

        with open(html_doc, "r", encoding="utf-8") as infile:
            raw = infile.read()

        soup = BeautifulSoup(raw, "html.parser")

        article_body = "".join(
            soup.select(self.get_selector(html_doc), limit=1)[0].find_all(string=True)
        )

        clean_article_body = re.sub(r'[^\w ,.-~;]+', '', article_body)

        print(clean_article_body)

    @staticmethod
    def validate_directory(dir):
        if os.path.isdir(os.path.abspath(dir)):
            return os.path.abspath(dir)
        raise DirectoryNotFound(f"{dir}/")


def main():
    e = Extractor("raw_docs", "./utils/links.csv")

    e.find_all_files()
    
    e.extract_main(e.html_raw[9])
 


if __name__ == "__main__":
    main()
