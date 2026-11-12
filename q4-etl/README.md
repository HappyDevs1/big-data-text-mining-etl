# Question 4: ETL with R and PostgreSQL

`q4_etl_books.Rmd` scrapes 100 books from books.toscrape.com, keeps the raw text, cleans it with regular expressions, imputes missing categories, exports `data/processed/q4_books_clean.csv`, loads it into PostgreSQL and runs two SQL investigations.

| File | Purpose |
|---|---|
| `sql/01_create_and_load.sql` | Creates the `books` table and imports the CSV with `\copy` |
| `sql/02_rating_vs_price_and_stock.sql` | Does rating relate to price or stock |
| `sql/03_category_value_ranking.sql` | Stock value by category and top priced title per category |
| `sql/output/` | Query results as printed by psql |

Run the SQL by hand:

```bash
createdb itbda_etl
psql -d itbda_etl -f sql/01_create_and_load.sql
psql -d itbda_etl -f sql/02_rating_vs_price_and_stock.sql
psql -d itbda_etl -f sql/03_category_value_ranking.sql
```
