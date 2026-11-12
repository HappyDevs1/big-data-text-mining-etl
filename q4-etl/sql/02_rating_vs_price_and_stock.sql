-- Investigation 1: do better rated books cost more or carry more stock?
-- Summarises the catalogue by star rating and adds the overall
-- correlation of rating with price and with stock.
WITH overall AS (
    SELECT ROUND(CORR(rating, price_gbp)::numeric, 3)   AS corr_rating_price,
           ROUND(CORR(rating, stock_count)::numeric, 3) AS corr_rating_stock,
           COUNT(*)                                     AS total_books
    FROM books
)
SELECT b.rating,
       COUNT(*)                                        AS books,
       ROUND(100.0 * COUNT(*) / o.total_books, 1)      AS pct_of_catalogue,
       ROUND(AVG(b.price_gbp), 2)                      AS avg_price_gbp,
       ROUND(AVG(b.stock_count), 1)                    AS avg_stock,
       SUM(b.price_gbp * b.stock_count)                AS stock_value_gbp,
       o.corr_rating_price,
       o.corr_rating_stock
FROM books b
CROSS JOIN overall o
GROUP BY b.rating, o.total_books, o.corr_rating_price, o.corr_rating_stock
ORDER BY b.rating;
