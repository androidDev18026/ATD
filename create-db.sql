DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE documents (
<<<<<<< HEAD
    id SMALLINT PRIMARY KEY,
    body TEXT NOT NULL,
    filepath VARCHAR ( 30 ) NOT NULL,
    length SMALLINT NOT NULL,
    size_kb INT NOT NULL,
    time_crawled TIMESTAMP WITHOUT TIME ZONE,
    doc_url VARCHAR ( 100 )
);

/* With Header */
COPY documents FROM PROGRAM 'awk FNR-1 /home/panos/Documents/ATD/docengine/raw_docs/doc*.csv | cat' DELIMITER ',' CSV;

/* Add Ts Vector column */
ALTER TABLE documents ADD COLUMN docvec TSVECTOR;

/* Convert column to vector type (Greek config) */
UPDATE documents SET docvec = to_tsvector('greek', body);

/* Drop original column */
ALTER TABLE documents DROP COLUMN IF EXISTS body CASCADE;
=======
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
>>>>>>> e7133e3 (initial commit)
