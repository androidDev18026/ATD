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
    nfiles = 0
    if check_dir_exists(dirname):
        nfiles = len([*os.scandir(os.path.abspath(dirname))])
    else:
        logger.warning("Directory %s not in the current path", dirname)

    return nfiles if nfiles else 0


def check_sync(dirname: str, outfile: str):
    if os.path.exists(outfile) and os.path.isfile(outfile):
        logger.info("%s present in the current path", outfile)
        with open(os.path.abspath(outfile)) as infile:
            clines = sum(1 for _ in infile) - 1
    else:
        logger.warning("%s not present in the current path", outfile)

    if clines:
        logger.info("Found %d lines in csv file", clines)
        ndir = get_num_files(dirname)
        diff = abs(ndir - clines)
        logger.warning("Difference of %d lines", diff)
    else:
        raise RuntimeWarning("File is empty, this shouldn't be the case")

    return ndir if diff else 0


def read_df(path: str, dirpath: str, override: bool = False) -> Tuple[pd.DataFrame, int] | NoneType:

    ndir = check_sync(dirpath, path)
    dir_path_present = os.path.exists(dirpath)

    if override:
        logger.warning("Directory not empty but user requested to overwrite it")
    else:
        if ndir:
            logger.info("Found mismatch, going to start reading file from line %d", ndir)
        elif not dir_path_present:
            with open(os.path.abspath(path)) as infile:
                ndir = sum(1 for _ in infile) - 1
        else:
            if not get_num_files(dirpath):
                pass
            else:
                logger.info("Same number of lines and files detected, not updating...")
                return NoneType
        

    try:
        df = pd.read_csv(
            path,
            sep=",",
            encoding="utf-8",
            header=0,
            names=("id", "title", "body"),
            skiprows=ndir if not override and ndir and dir_path_present else 0,
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
        logger.warning("%s not a directory", outdir)
        logger.info("Creating directory %s...", outdir)
        os.mkdir(os.path.join(os.path.curdir, outdir))
        logger.info("Created new directory %s", outdir)

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

    assert len(sys.argv) == 4, "Not enough arguments: <outfile.csv> <outdir> <0(no override)/1(override)>"

    overwrite = bool(int(sys.argv[3]))

    ret = read_df(sys.argv[1], sys.argv[2], override=overwrite)

    if ret is not NoneType:
        df, nlines = ret
        write_article(df, sys.argv[2])
    else:
        pass

    logger.info("Done")
