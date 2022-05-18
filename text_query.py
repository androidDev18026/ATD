import logging
import os
import re
import sys
from collections import defaultdict
from configparser import ConfigParser
from types import NoneType
from typing import Dict, List, NamedTuple

import psycopg
from psycopg import sql
from psycopg.rows import namedtuple_row
from greek_stemmer.stemmer import stem_word
from nltk.corpus import stopwords

from utils.call_grep import execute_cmd

import numpy as np

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s: %(message)s",
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
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


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
    
    query = (
        f"SELECT {','.join(columns)}, ts_rank_cd(docvec, query, {metric}) AS rank \
    FROM documents, plainto_tsquery('greek', '{user_input.strip()}') query \
    WHERE query @@ docvec \
    ORDER BY rank DESC"
    )

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

    if result_set:
        logger.info(
            "Found %d docs out of the %d requested", len(result_set), max_res
        )
    else:
        logger.warning("Got no results!")

    return result_set


def normalize_rank(results: List[NamedTuple]) -> List[NamedTuple]:
    """Normalize ranks in range [0,1]"""
    
    def normalize(data: List[float]) -> List[float]:
        return (data - np.min(data)) / (np.max(data) - np.min(data))        
    
    norm_ranks = normalize([row.rank for row in results])
    logger.info("Scaled ranks in range (0,1)")
    scaled_results = list()
    
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


def display_matching_line(query: str, filename: str):
    query = [word for word in query.split() if word not in stopwords.words("greek")]
    keywords = [stem_word(word, "NNM").lower() for word in query]        
    
    matching_lines = execute_cmd(filename, *keywords)    
    
    logger.info("Found %d matching lines in %s", matching_lines.__len__(), filename)
        
    for row in matching_lines:
        line = []
        for word in row.value.replace('.', ' ').split():
            if stem_word(word, 'NNM').lower() in keywords:
                line += [f"{bcolors.OKGREEN}{bcolors.BOLD}{word}{bcolors.ENDC}"]
            else:
                line += [word]

        print(f"Found match in line {row.lineno} -> {' '.join(l for l in line)}")
        
        
def find_relevant(results: List[NamedTuple], threshold: float = 0.5) -> int:
    return sum(1 for i in results if i.rank >= threshold)
    
    
# display all available metrics
def validate_metric(metric: str, default: str = "no_doc_length") -> int:
    if metric in VALID_METRICS.keys():
        logger.info("Metric chosen [%s]", metric)
        return VALID_METRICS[metric]
        
    logger.info("Available metrics: %s", ", ".join(f"'{str(i)}'" for i in VALID_METRICS.keys()))
    logger.warning("Invalid metric found, falling back to default '%s'", default)
    
    return 0


if __name__ == "__main__":
    
    config = read_from_config("postgre.ini")

    connection = initialize_conn(config)

    assert len(sys.argv) > 2, "Not enough arguments: <query> <metric> <max_res>"

    query, metric, max_res = sys.argv[1], sys.argv[2], sys.argv[3]

    query_str = query
    
    metric_ = validate_metric(metric)

    cols_to_display = ("title", "filepath")
    logger.info(f"Showing cols {*cols_to_display,}")
    
    query = prep_query(query, *cols_to_display, metric=metric_)
    
    results = execute_similarity_query(query, connection, int(max_res))
    scaled_results = normalize_rank(results)
    
    thres: float = 0.5
    
    logger.info("Using threshold to classify document as recommended: >= %.1f", thres)    
    logger.info("Recommended Docs: %d/%d", find_relevant(scaled_results, threshold=thres), results.__len__())
    
    display_results(scaled_results)
    display_matching_line(query_str, '/home/panos/Documents/ATD/docengine/raw_articles/article124.txt')
    
    connection.close()
    logger.info("Connection to database closed")
