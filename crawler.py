import os
import sys
import logging
import pandas as pd

from requests import Response
from tqdm import tqdm
from requests_threads import AsyncSession
from typing import List

session = AsyncSession(n=200)

logger = logging.getLogger()
logger.setLevel("INFO")


class PoliticsCrawler:
    def __init__(self, links_path: str) -> None:
        self.links = PoliticsCrawler.validate_file(links_path)
        self.responses = []

    def __str__(self) -> str:
        return f"Crawler reading from {self.links}"

    def __repr__(self) -> str:
        pass

    def read_from_file(self):
        self.df_links = pd.read_csv(
            self.links,
            sep=",",
            encoding="utf-8",
            skip_blank_lines=True,
            index_col=0,
            header=0,
        )

    async def get_raw_html_and_write(self):
        for link in tqdm(
            self.df_links.url,
            desc="Downloading... ",
            mininterval=0.05,
            colour="blue",
            ascii=True,
            dynamic_ncols=True,
        ):
            self.responses.append(
                await session.get(link, allow_redirects=False, timeout=5)
            )  # add status check

        await PoliticsCrawler.write_to_files(self.responses)

        logger.info("Done fetching and writing to output directory")

    @staticmethod
    async def write_to_files(responses: List[Response], basedir="raw_docs"):

        dir_to_write = os.path.join(os.path.curdir, basedir)

        for idx, res in enumerate(responses):
            with open(
                file=f"{os.path.join(dir_to_write, 'doc'+str(idx)+'.html')}",
                mode="w",
                encoding="utf-8",
            ) as out:
                out.write(res.text)

    @staticmethod
    def validate_file(path) -> str:
        path = os.path.abspath(path)
        if os.path.exists(path) and os.path.isfile(path):
            return path

        raise FileNotFoundError(f"{path} doesn't exist")


def main():

    logging.basicConfig(
        format="[%(levelname)s] %(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p"
    )

    logger.info("Started crawler...")

    cr = PoliticsCrawler(sys.argv[1])

    cr.read_from_file()

    session.run(cr.get_raw_html_and_write)


if __name__ == "__main__":
    main()
