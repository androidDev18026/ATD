import logging
import os
import re
import unicodedata
import sys
from collections import defaultdict
from configparser import ConfigParser
from pathlib import PurePath
from types import NoneType
from typing import Dict, List, NamedTuple

import numpy as np
import psycopg
from greek_stemmer.stemmer import stem_word
from nltk.corpus import stopwords
from psycopg import sql
from psycopg.rows import namedtuple_row

from utils.call_grep import execute_cmd

logging.basicConfig(
    format="[%(levelname)-7s] %(asctime)s: %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger()

MAX_RESULTS = 100
VALID_METRICS = {
    "no_doc_length": 0,
    "div_rank_by_1_log": 1,
    "div_doc_length": 2,
    "harmonic_dist": 4,
    "div_unique": 8,
    "div_rank_by_1_log_unique": 16,
    "div_rank_1": 32,
}


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def read_from_config(conf_file: str) -> Dict[str, str] | NoneType:

    config = ConfigParser(allow_no_value=False)

    try:
        config.read(conf_file)
        logger.info("Reading from %s", os.path.abspath(conf_file))
    except FileNotFoundError as e:
        logger.error("File doesn't exist %s", os.path.abspath(conf_file))

    db_conn = defaultdict()

    if config.has_section("credentials"):
        if config.has_option("credentials", "user"):
            db_conn["user"] = config.get("credentials", "user")
            logger.info("Using username [%s]", db_conn["user"])
        if config.has_option("credentials", "password"):
            db_conn["password"] = config.get("credentials", "password")
            logger.info("Using password [%s]", "*" * 6)
        if config.has_option("credentials", "host"):
            db_conn["host"] = config.get("credentials", "host")
            logger.info("Using host [%s]", db_conn["host"])
        if config.has_option("credentials", "port"):
            db_conn["port"] = config.get("credentials", "port")
            logger.info("Using port [%s]", db_conn["port"])
        if config.has_option("credentials", "dbname"):
            db_conn["dbname"] = config.get("credentials", "dbname")
            logger.info("Using database [%s]", db_conn["dbname"])
    else:
        raise RuntimeError("Configuration file missing parameters")

    if db_conn.__len__() >= 5:
        return db_conn

    logger.error("Insufficient number of connection parameters.")


def initialize_conn(conf_dict: Dict) -> psycopg.Connection:

    try:
        conn = psycopg.connect(**conf_dict)
        logger.info("Established connection with database %s", conf_dict["dbname"])
        return conn
    except psycopg.OperationalError as e:
        logger.error("Failed to connect to %s", conf_dict["dbname"])
        raise e


def prep_query(user_input: str, *columns: str, metric: int = 0) -> str:
    
    user_input = re.sub("\W", " ", user_input)
    user_input = re.sub("\s\s+", " ", user_input)
    
    logger.info("User searched for [%s]", user_input)

    query = f"SELECT {','.join(columns)}, ts_rank_cd(docvec, query, {metric}) AS rank \
    FROM documents, plainto_tsquery('greek', '{user_input.strip()}') query \
    WHERE query @@ docvec \
    ORDER BY rank DESC"

    logger.info("Constructed the query")

    return query


def execute_similarity_query(
    query: sql.SQL, connection: psycopg.Connection, max_res: int
) -> List[NamedTuple]:
    """Execute and return top k docs

    Args:
        query (sql.SQL): query
        connection (psycopg.Connection): connector
        max_res (int): top k most relevant
    """
    if max_res > MAX_RESULTS:
        raise ValueError(
            f"Results set exceeds max number of instances to return {max_res} > {MAX_RESULTS}"
        )

    result_set = []

    logger.info("Fetching at most %d instance(s)", max_res)
    with connection.cursor("conn", row_factory=namedtuple_row) as cur:
        cur.execute(query)
        for row in cur.fetchmany(max_res):
            result_set += [row]

        
    return result_set


def normalize_rank(results: List[NamedTuple]) -> List[NamedTuple]:
    """Normalize ranks in range [0,1]"""

    def normalize(data: List[float]) -> List[float]:
        return (data - np.min(data)) / (np.max(data) - np.min(data))\
            if any(np.diff(data)) else data
    
    norm_ranks = normalize([row.rank for row in results])
    logger.info("Scaled ranks in range (0,1)")
    scaled_results: List[NamedTuple] = []
    for i, row in enumerate(results):
        copy_Row = row._replace(rank=norm_ranks[i])
        scaled_results += [copy_Row]

    return scaled_results


def display_results(results: List[NamedTuple]) -> None:
    from tabulate import tabulate

    if results:
        print(
            tabulate(
                results,
                results[0]._fields,
                tablefmt="psql",
                floatfmt=".5f",
                showindex=True,
            )
        )
    else:
        logger.warning("Got an empty list, nothing to display")


def display_matching_line(
    query: str, filename: str, lang: str = "greek", cutoff: int = 5
) -> NoneType:

    query = [word for word in query.split() if word not in stopwords.words(lang)]
    keywords = list()

    for word in query:
        if len(word) >= cutoff:
            keywords += [stem_word(word, "NNM").lower() + "*"]
        else:
            keywords += [word.lower()]
    
    matching_lines = execute_cmd(filename, *keywords)
    color_word = lambda word: f"{bcolors.OKGREEN}{bcolors.BOLD}{word}{bcolors.ENDC}"

    if matching_lines:
        logger.info("Found %d matching lines in %s", matching_lines.__len__(), filename)
        keywords = [k.replace("*", "") for k in keywords]

        for row in matching_lines:
            line = []
            for word in row.value.replace(".", " ").split():
                if stem_word(word, "NNM").lower() in keywords or word in keywords:
                    line += [color_word(word)]
                elif any(
                    stem_word(word, "NNM").lower().__len__() >= len(w)
                    for w in keywords
                    if len(w) >= cutoff
                ) and any(word.find(k) != -1 for k in keywords):
                    line += [color_word(word)]
                else:
                    line += [word]

            print(
                f"Found match in line {row.lineno:3} -> {' '.join(l for l in line)}",
                end="\n",
            )
    else:
        logger.warning("Got an empty response from grep")


def display_matching_lines(
    results: List[NamedTuple], query: str, thres: float = 0.5
) -> NoneType:
    assert results, "Got 0 responses from DB, cannot find any matches"

    filepaths = [row.filepath for row in results if row.rank >= thres]
    valid_ranks = [row.rank for row in results if row.rank >= thres]

    def user_input(prompt: str = "\nShow More? (Y/N) : ") -> bool:
        res = input(prompt).strip().lower()
        if res in ("y", "yes"):
            return True
        elif res in ("n", "no"):
            return False
        return user_input(prompt)

    prompt1 = "\nShow matching lines in the documents retrieved with specified keywords? (Y/N) : "
    get_input = user_input(prompt=prompt1)

    if get_input:
        logger.info(
            "User has requested to show matching lines for keyword(s) [%s]", query
        )

        for path, rank in zip(filepaths, valid_ranks):
            logger.info(
                "Showing matches inside file %s with rank %.5f",
                PurePath(path).name,
                rank,
            )
            display_matching_line(query, path)
            if not user_input():
                logging.warning("User requested to halt execution")
                break
        logger.info("Nothing more to show...")
    else:
        logger.info("Skipping the display of matching lines")


def find_relevant(results: List[NamedTuple], threshold: float = 0.5) -> int:
    return sum(1 for i in results if i.rank >= threshold)


# display all available metrics
def validate_metric(metric: str, default: str = "no_doc_length") -> int:
    if metric in VALID_METRICS.keys():
        logger.info("Metric chosen [%s]", metric)
        return VALID_METRICS[metric]

    logger.info(
        "Available metrics: %s", ", ".join(f"'{str(i)}'" for i in VALID_METRICS.keys())
    )
    logger.warning("Invalid metric found, falling back to default '%s'", default)

    return 0


if __name__ == "__main__":

    config = read_from_config("postgre.ini")

    connection = initialize_conn(config)

    assert len(sys.argv) > 2, "Not enough arguments: <query> <metric> <max_res>"

    query, metric, max_res = sys.argv[1], sys.argv[2], sys.argv[3]

    query_str = query

    metric_ = validate_metric(metric)

    cols_to_display = ("title", "filepath")#, "docvec")
    logger.info(f"Showing cols {*cols_to_display,}")

    query = prep_query(query, *cols_to_display, metric=metric_)
    
    try:    
        results = execute_similarity_query(query, connection, int(max_res))
        scaled_results = normalize_rank(results)
        
        thres: float = 0.5

        logger.info("Found %d docs out of the %d requested", len(results), int(max_res))
        logger.info("Using threshold to classify document as recommended: >= %.1f", thres)
        logger.info(
            "Recommended Docs: %d/%d",
            find_relevant(scaled_results, threshold=thres),
            results.__len__(),
            )

        display_results(scaled_results)
        display_matching_lines(scaled_results, query_str, thres)
    
    except (ValueError, AssertionError):
        logger.error("Got 0 results!")
            

    finally:    
        connection.close()
        logger.info("Connection to database closed")
