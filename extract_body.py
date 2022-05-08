import logging
import os
import sys
from pathlib import Path
from types import NoneType
from typing import Tuple

import pandas as pd

from extract import DirectoryNotFound

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s : %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p",
    level="INFO",
)
logger = logging.getLogger()


def check_dir_exists(dirname: str) -> bool:
    if os.path.exists(dirname) and os.path.isdir(dirname):
        logger.info("Directory %s is present", dirname)
        return True

    return False


def get_num_files(dirname: str) -> int:
    if check_dir_exists(dirname):
        nfiles = len([*os.scandir(os.path.abspath(dirname))])
    else:
        logger.warning("Directory %s not in the current path", dirname)

    return nfiles if nfiles else 0


def check_sync(dirname: str, outfile: str):
    if os.path.exists(outfile) and os.path.isfile(outfile):
        logger.info("%s present in the current path", outfile)
        with open(os.path.abspath(outfile)) as infile:
            clines = sum(1 for _ in infile)
    else:
        logger.warning("%s not present in the current path", outfile)

    if clines:
        logger.info("Found %d lines in csv file", clines)
        ndir = get_num_files(dirname)
        logger.warning("Difference of %d lines", abs(ndir - clines))
    else:
        raise RuntimeWarning("File is empty, this shouldn't be the case")

    return ndir


def read_df(path: str, dirpath: str) -> Tuple[pd.DataFrame, int] | NoneType:

    ndir = check_sync(dirpath, path)

    if ndir:
        logger.info("Found mismatch, going to start reading file from line %d", ndir)

    try:
        df = pd.read_csv(
            path,
            sep=",",
            encoding="utf-8",
            header=0,
            names=("id", "title", "body"),
            skiprows=ndir,
            skip_blank_lines=True,
            usecols=("id", "title", "body"),
            index_col=0,
        )

        df.drop(columns=["title"], inplace=True)

        logger.info("Extraction Done")
        return (df, ndir)
    except FileNotFoundError as e:
        logger.error("File %s not found", e.filename)
    except OSError as o:
        logger.error(o.strerror)


def write_article(df: pd.DataFrame, outdir: str) -> None:
    if not os.path.isdir(outdir):
        logger.error("%s not a directory", outdir)
        raise DirectoryNotFound(outdir)

    logger.info("Writing to %s...", os.path.abspath(outdir))

    for i, txt in df.iterrows():
        fname = f"article{i}.txt"
        with open(os.path.join(outdir, fname), mode="w", encoding="utf-8") as out:
            res = out.write("".join(txt.values))
            if res:
                logger.info("Wrote %s succesfully", fname)
            else:
                logger.warning("Couldn't write %s", fname)


if __name__ == "__main__":

    logger.info("Starting %s", Path(__file__).stem)

    assert len(sys.argv) == 3, "Not enough arguments: <outfile.csv> <outdir>"

    df, nlines = read_df(sys.argv[1], sys.argv[2])

    write_article(df, sys.argv[2])

    logger.info("Done")
