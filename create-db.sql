DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE documents (
    id SMALLINT PRIMARY KEY,
    body TEXT NOT NULL,
    filepath VARCHAR ( 100 ) NOT NULL,
    length SMALLINT NOT NULL,
    size_kb INT NOT NULL,
    doc_url VARCHAR ( 250 ),
    time_crawled TIMESTAMP WITHOUT TIME ZONE
);

/* With Header */
COPY documents FROM PROGRAM 'awk FNR-1 ':'CSV_PATH'' | cat' DELIMITER ',' CSV;

/* Add Ts Vector column */
ALTER TABLE documents ADD COLUMN docvec TSVECTOR;

/* Convert column to vector type (Greek config) */
UPDATE documents SET docvec = to_tsvector('greek', body);

ALTER TABLE documents DROP COLUMN IF EXISTS body CASCADE;