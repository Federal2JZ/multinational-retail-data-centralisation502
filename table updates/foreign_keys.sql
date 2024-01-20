-- Find all card_numbers in orders_table that are not assigned to dim_card_details
SELECT orders_table.card_number 
FROM orders_table
LEFT JOIN dim_card_details
ON orders_table.card_number = dim_card_details.card_number
WHERE dim_card_details.card_number IS NULL;


-- Delete mismatched rows
DELETE FROM orders_table WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);
DELETE FROM orders_table WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);
DELETE FROM orders_table WHERE product_code NOT IN (SELECT product_code FROM dim_products);


-- Insert all card_numbers from orders_tale not in dim_card_details into dim_card_details
INSERT INTO dim_card_details (card_number)
SELECT DISTINCT orders_table.card_number
FROM orders_table
WHERE orders_table.card_number NOT IN 
	(SELECT dim_card_details.card_number
	FROM dim_card_details);


-- Add the foreign keys to the orders table
ALTER TABLE orders_table
	ADD FOREIGN KEY (card_number)
	REFERENCES dim_card_details(card_number);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times(date_uuid);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (product_code)
	REFERENCES dim_products(product_code);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (store_code)
	REFERENCES dim_store_details(store_code);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (user_uuid)
	REFERENCES dim_users(user_uuid);