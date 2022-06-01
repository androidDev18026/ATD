/* 
    1. Using enviroment variable CSV_PATH to determine the location of the csv file after running extract.py

    2. To add the links to the actual articles append perform another copy from the second enviroment vaiable
       LOCAL_PATH using the csv file after running get_local_link.py
       
    Example: $ psql -U postgres -f create-db.sql -d test_db -a -v CSV_PATH=/path/to/outfile.csv -v LOCAL_PATH=/path/to/article_path.csv
*/

DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE documents (
    id SMALLINT UNIQUE PRIMARY KEY,
    body TEXT NOT NULL,
    title VARCHAR ( 200 ) NOT NULL,
    filepath VARCHAR ( 100 ) NULL,
    length SMALLINT NOT NULL DEFAULT 0,
    size_kb INT NOT NULL DEFAULT 0,
    doc_url VARCHAR ( 250 ),
    time_crawled TIMESTAMP WITHOUT TIME ZONE
);

/* With Header */
COPY documents(id, title, body, length, size_kb, doc_url, time_crawled) 
FROM PROGRAM 'awk FNR-1 ':'CSV_PATH'' | cat' DELIMITER ',' CSV;

/* Create a temp table to load the actual paths stored locally */
CREATE TEMPORARY TABLE temp_dest (id SMALLINT, filepath_ VARCHAR ( 100 ) NULL);
COPY temp_dest FROM PROGRAM 'awk FNR-1 ':'LOCAL_PATH'' | cat' DELIMITER ',' CSV;

/* Add Ts Vector column */
ALTER TABLE documents ADD COLUMN docvec TSVECTOR;

/* Convert column to vector type (Greek config) */
UPDATE documents SET docvec = to_tsvector('greek', body);

/* Merge the temp table with the original to get the paths */
UPDATE documents SET filepath = (select filepath_ from temp_dest where documents.id = temp_dest.id);

/* Raw text files should not be present in the database so we can drop that column
   and only keep the one with texts represente as vectors */ 
ALTER TABLE documents DROP COLUMN IF EXISTS body CASCADE; 

CREATE INDEX docvec_idx ON documents USING GIN (docvec);