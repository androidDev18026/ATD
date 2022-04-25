DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE documents (
    id SMALLINT PRIMARY KEY,
    body VARCHAR NOT NULL,
    filepath VARCHAR ( 30 ) NOT NULL,
    length SMALLINT NOT NULL,
    size_kb INT NOT NULL,
    time_crawled TIMESTAMP WITHOUT TIME ZONE
);

/* With Header */
COPY documents FROM PROGRAM 'awk FNR-1 /home/panos/Documents/ATD/docengine/raw_docs/doc*.csv | cat' DELIMITER ',' CSV;

/* Add Ts Vector column */
ALTER TABLE documents ADD COLUMN docvec TSVECTOR;

/* Convert column to vector type */
UPDATE documents SET docvec = to_tsvector(body);

/* Drop original column */
ALTER TABLE documents DROP COLUMN IF EXISTS body CASCADE;