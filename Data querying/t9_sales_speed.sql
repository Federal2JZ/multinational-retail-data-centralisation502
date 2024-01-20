-- How quickly is the company making sales?

WITH timestamp_table AS (
    SELECT
        MAKE_TIMESTAMP(time_table.year::int, time_table.month::int, time_table.day::int,
                       time_table.hour::int, time_table.minutes::int, time_table.seconds::float) AS order_timestamp,
        time_table.date_uuid AS date_uuid,
        time_table.year::int AS year
    FROM (
        SELECT
            EXTRACT(hour FROM CAST(timestamp AS TIME)) AS hour,
            EXTRACT(minute FROM CAST(timestamp AS TIME)) AS minutes,
            EXTRACT(second FROM CAST(timestamp AS TIME)) AS seconds,
            day,
            month,
            year,
            date_uuid
        FROM dim_date_times
    ) AS time_table
),
time_stamp_diffs AS (
    SELECT
        year,
        order_timestamp - LAG(order_timestamp) OVER (ORDER BY order_timestamp ASC) AS time_diff
    FROM orders_table
    JOIN timestamp_table ON orders_table.date_uuid = timestamp_table.date_uuid
),
year_time_diffs AS (
    SELECT
        year,
        AVG(time_diff) AS average_time_diff
    FROM time_stamp_diffs
    GROUP BY year
    ORDER BY average_time_diff DESC
)

SELECT
    year,
    CONCAT(
        'hours: ', EXTRACT(HOUR FROM average_time_diff),
        '  minutes: ', EXTRACT(MINUTE FROM average_time_diff),
        '  seconds: ', CAST(EXTRACT(SECOND FROM average_time_diff) AS INT),
        '  milliseconds: ', CAST(EXTRACT(MILLISECOND FROM average_time_diff) AS INT)
    ) AS actual_time_taken
FROM year_time_diffs;