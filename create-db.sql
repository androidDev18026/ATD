/* 
    1. Using enviroment variable CSV_PATH to determine the location of the csv file
    Define it in the psql init script like : # psql -d db_name -f create-db.sql -e -v CSV_PATH=/path/to/csv

    2. To add the links to the actual articles append perform another copy from the second enviroment vaiable
       LOCAL_PATH
*/

DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE documents (
    id SMALLINT PRIMARY KEY,
    body TEXT NOT NULL,
    filepath VARCHAR ( 100 ) NULL,
    length SMALLINT NOT NULL DEFAULT 0,
    size_kb INT NOT NULL DEFAULT 0,
    doc_url VARCHAR ( 250 ),
    time_crawled TIMESTAMP WITHOUT TIME ZONE
);

/* With Header */
COPY documents(id, body, length, size_kb, doc_url, time_crawled) 
FROM PROGRAM 'awk FNR-1 ':'CSV_PATH'' | cat' DELIMITER ',' CSV;

CREATE TEMPORARY TABLE temp_dest (id SMALLINT, filepath_ VARCHAR ( 100 ) NULL);
COPY temp_dest FROM PROGRAM 'awk FNR-1 ':'LOCAL_PATH'' | cat' DELIMITER ',' CSV;

/* Add Ts Vector column */
ALTER TABLE documents ADD COLUMN docvec TSVECTOR;

/* Convert column to vector type (Greek config) */
UPDATE documents SET docvec = to_tsvector('greek', body);
UPDATE documents SET filepath = (select filepath_ from temp_dest where documents.id = temp_dest.id);

ALTER TABLE documents DROP COLUMN IF EXISTS body CASCADE; 