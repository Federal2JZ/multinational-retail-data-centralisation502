-- Which month in each year produced the highest cost of sales?

SELECT
    ROUND(SUM(ot.product_quantity * dp.product_price)::numeric, 2) AS total_sales,
    dd.year,
    dd.month
FROM
    orders_table ot
LEFT JOIN
    dim_date_times dd ON ot.date_uuid = dd.date_uuid
LEFT JOIN
    dim_products dp ON ot.product_code = dp.product_code
GROUP BY
    dd.year, dd.month
ORDER BY
    total_sales DESC
LIMIT 10;