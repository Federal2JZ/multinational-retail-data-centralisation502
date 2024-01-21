--Find max length of both card_number and expiry_date
SELECT
    MAX(LENGTH(card_number::TEXT)) AS max_card_number_length, -- =19
    MAX(LENGTH(expiry_date::TEXT)) AS max_expiry_date_length -- =5
FROM dim_card_details;


-- Alters data types in the table
ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE USING CAST(date_payment_confirmed as DATE);