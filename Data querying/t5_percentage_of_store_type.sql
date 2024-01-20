-- What percentage of sales come through each type of store?

SELECT
    ds.store_type,
    ROUND(CAST(SUM(ot.product_quantity * dp.product_price) AS NUMERIC), 2) AS total_sales,
    ROUND(CAST((SUM(ot.product_quantity * dp.product_price) / SUM(SUM(ot.product_quantity * dp.product_price)) OVER ()) * 100 AS NUMERIC), 2) AS percentage_total
FROM
    orders_table ot
LEFT JOIN
    dim_store_details ds ON ot.store_code = ds.store_code
LEFT JOIN
    dim_products dp ON ot.product_code = dp.product_code
GROUP BY
    ds.store_type
ORDER BY
    total_sales DESC;