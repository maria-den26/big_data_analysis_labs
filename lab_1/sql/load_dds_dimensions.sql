--sql для заполнения 4 измерений


-- 1. Покупатели
INSERT INTO dds.dim_customers (
    customer_id,
    customer_name,
    customer_address,
    customer_birthday,
    customer_email
)
SELECT
    customer_id,
    customer_name,
    customer_address,
    customer_birthday,
    customer_email
FROM stg.craft_market_customers
    ON CONFLICT (customer_id) DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    customer_address = EXCLUDED.customer_address,
    customer_birthday = EXCLUDED.customer_birthday,
    customer_email = EXCLUDED.customer_email;


-- 2. Мастера
INSERT INTO dds.dim_craftsmans (
    craftsman_id,
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email
)
SELECT
    craftsman_id,
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email
FROM stg.craft_market_craftsmans
    ON CONFLICT (craftsman_id) DO UPDATE SET
    craftsman_name = EXCLUDED.craftsman_name,
    craftsman_address = EXCLUDED.craftsman_address,
    craftsman_birthday = EXCLUDED.craftsman_birthday,
    craftsman_email = EXCLUDED.craftsman_email;


-- 3. Товары
INSERT INTO dds.dim_products (
    product_id,
    product_name,
    product_description,
    product_type
)
SELECT DISTINCT ON (product_id)
    product_id,
    product_name,
    product_description,
    product_type
FROM stg.craft_market_orders
WHERE product_id IS NOT NULL
ORDER BY product_id, order_created_date DESC NULLS LAST, order_id DESC
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    product_description = EXCLUDED.product_description,
    product_type = EXCLUDED.product_type;


-- 4. Календарь
INSERT INTO dds.dim_dates (
    date_key,
    year,
    quarter,
    month,
    day
)
SELECT
    d.date_key,
    EXTRACT(YEAR FROM d.date_key)::INTEGER,
    EXTRACT(QUARTER FROM d.date_key)::INTEGER,
    EXTRACT(MONTH FROM d.date_key)::INTEGER,
    EXTRACT(DAY FROM d.date_key)::INTEGER
FROM (
         SELECT order_created_date::DATE AS date_key
         FROM stg.craft_market_orders

         UNION

         SELECT order_completion_date::DATE AS date_key
         FROM stg.craft_market_orders
     ) d
WHERE d.date_key IS NOT NULL
    ON CONFLICT (date_key) DO NOTHING;



