-- Unknown members (key -1) so a fact row never needs a NULL foreign key
INSERT INTO dw.dim_customer (customer_key, customer_id, full_name, email, city, address, valid_from, valid_to, is_current)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '9999-12-31', true);

INSERT INTO dw.dim_product (product_key, product_id, product_name, category, list_price)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown', NULL);

INSERT INTO dw.dim_store (store_key, store_id, store_name, city)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown');

INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
VALUES (-1, NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL, 'Unknown', NULL);
