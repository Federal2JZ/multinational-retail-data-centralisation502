-- Find max length of country_code in dim_users
SELECT length(max(cast(country_code as Text)))
FROM dim_users
GROUP BY country_code
ORDER BY length(max(cast(country_code as Text))) desc
LIMIT 1; 
-- =3


-- Cast data types
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE using (date_of_birth::DATE),
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN join_date TYPE DATE;