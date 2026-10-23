--sql для заполнения таблицы fact_orders


INSERT INTO dds.fact_orders (
    order_id,
    customer_key,
    craftsman_key,
    product_key,
    created_date_key,
    completed_date_key,
    order_created_at,
    order_completed_at,
    order_status,
    product_price
)
SELECT
    o.order_id,
    c.customer_key,
    cr.craftsman_key,
    p.product_key,
    d1.date_key,
    d2.date_key,
    o.order_created_date,
    o.order_completion_date,
    o.order_status,
    o.product_price
FROM stg.craft_market_orders o
JOIN dds.dim_customers c
    ON o.customer_id = c.customer_id
JOIN dds.dim_craftsmans cr
    ON o.craftsman_id = cr.craftsman_id
JOIN dds.dim_products p
    ON o.product_id = p.product_id
JOIN dds.dim_dates d1
    ON o.order_created_date::DATE = d1.date_key
LEFT JOIN dds.dim_dates d2
    ON o.order_completion_date::DATE = d2.date_key

ON CONFLICT (order_id) DO UPDATE SET
    customer_key = EXCLUDED.customer_key,
                                  craftsman_key = EXCLUDED.craftsman_key,
                                  product_key = EXCLUDED.product_key,
                                  created_date_key = EXCLUDED.created_date_key,
                                  completed_date_key = EXCLUDED.completed_date_key,
                                  order_created_at = EXCLUDED.order_created_at,
                                  order_completed_at = EXCLUDED.order_completed_at,
                                  order_status = EXCLUDED.order_status,
                                  product_price = EXCLUDED.product_price;



