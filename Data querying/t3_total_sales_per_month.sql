-- Which months produced the largest amount of sales?

SELECT
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS numeric), 2) AS total_sales,
    dim_date_times.month
FROM
    orders_table
LEFT JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
LEFT JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
    dim_date_times.month
ORDER BY
    total_sales DESC
LIMIT 6;