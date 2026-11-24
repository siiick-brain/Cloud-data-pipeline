-- Loads dimensions from raw source tables. Safe to rerun: a second run changes nothing.

-- dim_customer is SCD Type 2 on city and address.
-- 1. close the current row when a tracked column changed
UPDATE dw.dim_customer d
SET valid_to = r.updated_at, is_current = false
FROM raw.customers r
WHERE d.customer_id = r.customer_id
  AND d.is_current
  AND d.customer_key <> -1
  AND (d.city, d.address) IS DISTINCT FROM (r.city, r.address);

-- 2. open a row for new customers and for the ones closed above.
-- First version starts at 1900-01-01 so older facts still find a row.
INSERT INTO dw.dim_customer (customer_id, full_name, email, city, address, valid_from)
SELECT r.customer_id, r.full_name, r.email, r.city, r.address,
       CASE WHEN EXISTS (SELECT 1 FROM dw.dim_customer h WHERE h.customer_id = r.customer_id)
            THEN r.updated_at ELSE '1900-01-01'::timestamptz END
FROM raw.customers r
WHERE NOT EXISTS (
    SELECT 1 FROM dw.dim_customer d WHERE d.customer_id = r.customer_id AND d.is_current
);

-- 3. name and email are Type 1, so overwrite them on the current row
UPDATE dw.dim_customer d
SET full_name = r.full_name, email = r.email
FROM raw.customers r
WHERE d.customer_id = r.customer_id
  AND d.is_current
  AND (d.full_name, d.email) IS DISTINCT FROM (r.full_name, r.email);

-- dim_product and dim_store are SCD Type 1
INSERT INTO dw.dim_product (product_id, product_name, category, list_price)
SELECT product_id, product_name, category, list_price FROM raw.products
ON CONFLICT (product_id) DO UPDATE
SET product_name = EXCLUDED.product_name, category = EXCLUDED.category, list_price = EXCLUDED.list_price
WHERE (dw.dim_product.product_name, dw.dim_product.category, dw.dim_product.list_price)
      IS DISTINCT FROM (EXCLUDED.product_name, EXCLUDED.category, EXCLUDED.list_price);

INSERT INTO dw.dim_store (store_id, store_name, city)
SELECT store_id, store_name, city FROM raw.stores
ON CONFLICT (store_id) DO UPDATE
SET store_name = EXCLUDED.store_name, city = EXCLUDED.city
WHERE (dw.dim_store.store_name, dw.dim_store.city) IS DISTINCT FROM (EXCLUDED.store_name, EXCLUDED.city);
