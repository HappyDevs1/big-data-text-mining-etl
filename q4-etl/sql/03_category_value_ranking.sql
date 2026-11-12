-- Investigation 2: which categories hold the most stock value, and what is
-- the top priced title in each? Uses window functions to rank categories
-- and to pick the most expensive book per category.
WITH ranked_books AS (
    SELECT category,
           title,
           price_gbp,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY price_gbp DESC) AS price_rank
    FROM books
),
category_summary AS (
    SELECT category,
           COUNT(*)                              AS books,
           ROUND(AVG(price_gbp), 2)              AS avg_price_gbp,
           ROUND(AVG(rating), 2)                 AS avg_rating,
           SUM(stock_count)                      AS units_in_stock,
           SUM(price_gbp * stock_count)          AS stock_value_gbp,
           SUM(category_imputed::int)            AS imputed_rows
    FROM books
    GROUP BY category
    HAVING COUNT(*) >= 3
)
SELECT RANK() OVER (ORDER BY c.stock_value_gbp DESC)                      AS value_rank,
       c.category,
       c.books,
       c.avg_price_gbp,
       c.avg_rating,
       c.units_in_stock,
       c.stock_value_gbp,
       ROUND(100.0 * c.stock_value_gbp / SUM(c.stock_value_gbp) OVER (), 1) AS pct_of_value,
       c.imputed_rows,
       r.title                                                             AS top_priced_title,
       r.price_gbp                                                         AS top_price_gbp
FROM category_summary c
JOIN ranked_books r
  ON r.category = c.category AND r.price_rank = 1
ORDER BY value_rank;
