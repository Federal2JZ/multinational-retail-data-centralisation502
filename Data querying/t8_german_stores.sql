-- Which German store type is selling the most?

SELECT
    ROUND(SUM(ot.product_quantity * dp.product_price)::numeric, 2) AS total_sales,
    ds.store_type,
    ds.country_code
FROM
    orders_table ot
LEFT JOIN
    dim_store_details ds ON ot.store_code = ds.store_code
LEFT JOIN
    dim_products dp ON ot.product_code = dp.product_code
WHERE
    ds.country_code = 'DE'
GROUP BY
    ds.store_type, ds.country_code
ORDER BY
    total_sales DESC;