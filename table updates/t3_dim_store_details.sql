-- Find max lengths
SELECT length(max(cast(country_code as Text)))
FROM dim_store_details
GROUP BY country_code
ORDER BY length(max(cast(country_code as Text))) desc
LIMIT 1; 
-- =2

SELECT length(max(cast(store_code as Text)))
FROM dim_store_details
GROUP BY store_code
ORDER BY length(max(cast(store_code as Text))) desc
LIMIT 1; 
-- =12


-- Update N/A values to NULL
UPDATE dim_store_details 
SET address = NULL
WHERE address = 'N/A';

UPDATE dim_store_details 
SET longitude = NULL
WHERE longitude = 'N/A';

UPDATE dim_store_details 
SET locality = NULL
WHERE locality = 'N/A';

UPDATE dim_store_details 
SET lat = NULL
WHERE lat = 'N/A';


-- Merge lat with latitude
UPDATE dim_store_details
SET latitude = CONCAT(CAST(lat as FLOAT), CAST(latitude as FLOAT));


-- Drop columns
ALTER TABLE dim_store_details
	DROP lat,
	DROP level_0;


-- Cast data types
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING CAST(longitude AS FLOAT),
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE USING CAST(opening_date as DATE),
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);