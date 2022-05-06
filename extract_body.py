import logging
import os
import sys
from pathlib import Path
from extract import DirectoryNotFound
import pandas as pd

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s : %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p",
    level="INFO",
)
logger = logging.getLogger()


def read_df(path: str) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(
            path,
            sep=",",
            encoding="utf-8",
            header=0,
            skip_blank_lines=True,
            usecols=("id", "body"),
            index_col=0,
        )
        logger.info("Extraction Done")
        return df
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

    df = read_df(sys.argv[1])

    write_article(df, sys.argv[2])

    logger.info("Done")
