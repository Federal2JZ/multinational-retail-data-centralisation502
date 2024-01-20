-- What is our staff headcount?

SELECT
    SUM(ds.staff_numbers) AS total_staff_numbers,
    ds.country_code
FROM
    dim_store_details ds
GROUP BY
    ds.country_code
ORDER BY
    total_staff_numbers DESC;