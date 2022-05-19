import logging
import os
import sys
from typing import List

import pandas as pd
from requests import Response
from requests_threads import AsyncSession
from tqdm import tqdm

session = AsyncSession(n=200)

logger = logging.getLogger()
logger.setLevel("INFO")


class PoliticsCrawler:
    def __init__(self, links_path: str, output_dir: str) -> None:
        self.links = PoliticsCrawler.validate_file(links_path)
        self.responses = []
        self.outdir = output_dir

    def __str__(self) -> str:
        return f"Crawler reading from {self.links}"

    def __repr__(self) -> str:
        pass

    def read_from_file(self):

        nfiles = self.check_empty_dir(self.outdir)
        nfiles = nfiles + 1 if nfiles else nfiles

        logger.info("Skipping %d rows", nfiles)

        self.df_links = pd.read_csv(
            self.links,
            sep=",",
            encoding="utf-8",
            skip_blank_lines=True,
            skiprows=nfiles,
            index_col=0,
            header=0 if not nfiles else None,
        )

        self.df_links.set_axis(["url"], axis=1, inplace=True)

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

        await self.write_to_files(self.responses)

        logger.info("Done fetching and writing to output directory")

    async def write_to_files(self, responses: List[Response]):

        dir_to_write = os.path.join(os.path.curdir, self.outdir)
        start = self.check_empty_dir(self.outdir)

        for idx, res in enumerate(responses):
            with open(
                file=f"{os.path.join(dir_to_write, f'doc{str(start + idx)}.html')}",
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

    @staticmethod
    def get_all_links(path: str):
        urls = []

        try:
            path = PoliticsCrawler.validate_file(path)

            with open(path, mode="r", encoding="utf-8") as infile:
                infile.readline()
                for line in infile.readlines():
                    urls += line.split(",")[1].splitlines(keepends=False)

            return urls

        except (FileNotFoundError):
            pass

    @staticmethod
    def check_empty_dir(dirname: str) -> int:
        if os.path.isdir(dirname):
            n_files = len([*os.scandir(os.path.abspath(dirname))])

        logger.info("Found %d files in %s", n_files, dirname)

        return n_files if n_files else 0


def main():

    logging.basicConfig(
        format="[%(levelname)s] %(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p"
    )

    assert len(sys.argv) == 3, "Not enough arguments: <links_path> <output dir>"

    logger.info("Started crawler...")

    cr = PoliticsCrawler(sys.argv[1], sys.argv[2])

    cr.read_from_file()

    session.run(cr.get_raw_html_and_write)


if __name__ == "__main__":
    main()
