-- How many sales are coming from online?

SELECT
    COUNT(ot.product_quantity) AS numbers_of_sales,
    SUM(ot.product_quantity) AS product_quantity_count,
    CASE
        WHEN ds.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table ot
LEFT JOIN
    dim_store_details ds ON ot.store_code = ds.store_code
GROUP BY
    location

-- Zero online sales showing in table for some reason