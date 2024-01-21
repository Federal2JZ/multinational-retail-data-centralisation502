--Find max length
SELECT
    MAX(LENGTH(month::TEXT)) AS max_month_length, -- =2
    MAX(LENGTH(year::TEXT)) AS max_year_length, -- =4
    MAX(LENGTH(day::TEXT)) AS max_day_length, -- =2
    MAX(LENGTH(time_period::TEXT)) AS max_time_period_length -- =10
FROM dim_date_times;


-- Alters data types in the table
ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2),
	ALTER COLUMN year TYPE VARCHAR(4),
	ALTER COLUMN day TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID);