-- Find max lengths
SELECT length(max(cast(card_number as Text)))
FROM orders_table
GROUP BY card_number
ORDER BY length(max(cast(card_number as Text))) desc
LIMIT 1; 
-- =19

SELECT length(max(cast(store_code as Text)))
FROM orders_table
GROUP BY store_code
ORDER BY length(max(cast(store_code as Text))) desc
LIMIT 1; -- =12

SELECT length(max(cast(product_code as Text)))
FROM orders_table
GROUP BY product_code
ORDER BY length(max(cast(product_code as Text))) desc
LIMIT 1; --  =11


-- Cast the data types in the table
ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
	ALTER COLUMN product_quantity TYPE SMALLINT;