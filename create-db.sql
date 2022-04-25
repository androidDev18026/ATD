DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE documents (
    id PRIMARY KEY,
    body VARCHAR NOT NULL,
    path VARCHAR ( 30 ) NOT NULL,
    length SMALLINT NOT NULL,
    size_kb INT NOT NULL,
    time_crawled TIMESTAMP WITHOUT TIME ZONE 
);

// With Header
COPY documents FROM PROGRAM 'awk FNR-1 /home/panos/Documents/ATD/docengine.csv | cat' DELIMITER ',' CSV;

// Add Ts Vector column
ALTER TABLE documents ADD COLUMN docvec TSVECTOR NOT NULL;

UPDATE documents SET docvec = (SELECT to_tsvector(body) FROM documents);