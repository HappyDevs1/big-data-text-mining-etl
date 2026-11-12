-- Question 4.3: create the table and import the cleaned CSV
-- Run from the q4-etl folder:  psql -d itbda_etl -f sql/01_create_and_load.sql

DROP TABLE IF EXISTS books;

CREATE TABLE books (
    upc                  CHAR(16)      PRIMARY KEY,
    title                TEXT          NOT NULL,
    title_was_truncated  BOOLEAN       NOT NULL,
    category             TEXT          NOT NULL,
    category_imputed     BOOLEAN       NOT NULL,
    price_gbp            NUMERIC(6, 2) NOT NULL CHECK (price_gbp > 0),
    price_band           TEXT          NOT NULL,
    rating               SMALLINT      NOT NULL CHECK (rating BETWEEN 1 AND 5),
    stock_count          INTEGER       NOT NULL CHECK (stock_count >= 0),
    stock_imputed        BOOLEAN       NOT NULL,
    description          TEXT          NOT NULL,
    description_imputed  BOOLEAN       NOT NULL,
    description_words    INTEGER       NOT NULL,
    url                  TEXT          NOT NULL,
    scraped_at           TIMESTAMP     NOT NULL
);

\copy books FROM '../data/processed/q4_books_clean.csv' WITH (FORMAT csv, HEADER true, NULL 'NA')

-- Quick checks that the import is complete
SELECT COUNT(*)                         AS rows_loaded,
       COUNT(DISTINCT category)         AS categories,
       SUM(category_imputed::int)       AS imputed_categories,
       SUM(description_imputed::int)    AS imputed_descriptions,
       MIN(price_gbp)                   AS min_price,
       MAX(price_gbp)                   AS max_price
FROM books;
