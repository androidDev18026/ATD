import sys
from typing import List, NamedTuple
import psycopg
import logging
import re
from psycopg.rows import namedtuple_row
from psycopg import sql


logging.basicConfig(
    format="[%(levelname)s] %(asctime)s: %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger()

MAX_RESULTS = 100


def initialize_conn(dbname, password, host="localhost", user="postgres", port="5432"):
    params = f"dbname={dbname} password={password} host={host} user={user} port={port}"

    try:
        conn = psycopg.connect(params)
        logger.info("Established connection with database %s", dbname)
        return conn
    except psycopg.OperationalError as e:
        logger.error("Failed to connect to %s", dbname)


def prep_query(user_input: str):

    user_input = re.sub("\W", " ", user_input)
    user_input = re.sub("\s\s+", " ", user_input)

    logger.info("User searched for [%s]", user_input)

    query = (
        "SELECT title, filepath, ts_rank_cd(docvec, query) AS rank \
    FROM documents, plainto_tsquery('greek', '%s') query \
    WHERE query @@ docvec \
    ORDER BY rank DESC"
        % user_input.strip()
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
            "Found %d relevant docs out of the %d requested", len(result_set), max_res
        )
    else:
        logger.warning("Got no results!")

    return result_set


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


if __name__ == "__main__":
    connection = initialize_conn("test_db", "1234")

    query = prep_query(sys.argv[1])
    max_res = int(sys.argv[2])

    results = execute_similarity_query(query, connection, max_res)

    display_results(results)

    connection.close()
    logger.info("Connection to database closed")
