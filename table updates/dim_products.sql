-- Remove the pound sign in the product price column
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');


-- Alter the column to a float type
ALTER TABLE dim_products 
	ALTER COLUMN weight TYPE FLOAT USING CAST(weight as FLOAT),
	ADD COLUMN weight_class VARCHAR;


-- Add text categories based on the weights of the products
UPDATE dim_products
SET weight_class =
	CASE 
		WHEN weight < 2.0 THEN 'Light'
		WHEN weight >= 2 
			AND weight < 40 THEN 'Mid_Sized'
		WHEN weight >= 40 
			AND weight <140 THEN 'Heavy'
		WHEN weight >= 140 THEN 'Truck_Required'
	END;
	
	
-- Find max lengths
SELECT length(max(cast(product_code as Text)))
FROM dim_products
GROUP BY product_code
ORDER BY length(max(cast(product_code as Text))) desc
LIMIT 1; -- =11

SELECT length(max(cast(weight_class as Text)))
FROM dim_products
GROUP BY weight_class
ORDER BY length(max(cast(weight_class as Text))) desc
LIMIT 1; -- =14

SELECT length(max(cast("EAN" as Text)))
FROM dim_products
GROUP BY "EAN"
ORDER BY length(max(cast("EAN" as Text))) desc
LIMIT 1; -- =17


-- Rename column in dim_products
ALTER TABLE dim_products 
	RENAME COLUMN removed to still_available;


-- Alter data types in the table
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT USING CAST(product_price as FLOAT),
	ALTER COLUMN weight TYPE FLOAT USING CAST(weight as FLOAT),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING CAST(date_added as DATE),
	ALTER COLUMN uuid TYPE UUID USING CAST(uuid as UUID),
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN weight_class TYPE VARCHAR(14),
	ALTER COLUMN still_available TYPE boolean USING (still_available ='Still_available');