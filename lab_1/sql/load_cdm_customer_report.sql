WITH monthly_stats AS (
    -- Основные показатели покупателя за месяц
    SELECT
        c.customer_id,
        c.customer_name,
        c.customer_address,
        c.customer_birthday,
        c.customer_email,

        DATE_TRUNC('month', f.order_created_at)::DATE AS report_period,

        -- Сумма завершённых заказов
        COALESCE(
                SUM(f.product_price) FILTER (
            WHERE f.order_status = 'done'
            ), 0
        ) AS customer_money,

        -- Комиссия платформы — 10%
        COALESCE(
                SUM(f.product_price) FILTER (
            WHERE f.order_status = 'done'
            ), 0
        ) * 0.10 AS platform_money,

        COUNT(*) AS count_order,
        ROUND(AVG(f.product_price), 2) AS avg_price_order,

        -- Медианное время выполнения в днях
        PERCENTILE_CONT(0.5) WITHIN GROUP (
    ORDER BY
    EXTRACT(EPOCH FROM (
    f.order_completed_at - f.order_created_at
    )) / 86400.0
    ) FILTER (
    WHERE f.order_status = 'done'
    AND f.order_completed_at IS NOT NULL
    ) AS median_time_order,

                                        -- Количество заказов по статусам
    COUNT(*) FILTER (
    WHERE f.order_status = 'created'
    ) AS count_order_created,

    COUNT(*) FILTER (
    WHERE f.order_status = 'in_progress'
    ) AS count_order_in_progress,

    COUNT(*) FILTER (
    WHERE f.order_status = 'delivery'
    ) AS count_order_delivery,

    COUNT(*) FILTER (
    WHERE f.order_status = 'done'
    ) AS count_order_done,

    COUNT(*) FILTER (
    WHERE f.order_status IN ('created', 'in_progress', 'delivery')
    ) AS count_order_not_done

FROM dds.fact_orders f

    JOIN dds.dim_customers c
ON f.customer_key = c.customer_key

GROUP BY
    c.customer_id,
    c.customer_name,
    c.customer_address,
    c.customer_birthday,
    c.customer_email,
    DATE_TRUNC('month', f.order_created_at)::DATE
    ),

    category_counts AS (
-- Считаем заказы каждой категории
SELECT
    c.customer_id,
    DATE_TRUNC('month', f.order_created_at)::DATE AS report_period,
    p.product_type,
    COUNT(*) AS order_count

FROM dds.fact_orders f
    JOIN dds.dim_customers c
ON f.customer_key = c.customer_key
    JOIN dds.dim_products p
    ON f.product_key = p.product_key

GROUP BY
    c.customer_id,
    DATE_TRUNC('month', f.order_created_at)::DATE,
    p.product_type
    ),

    top_categories AS (
-- Выбираем наиболее популярную категорию
SELECT
    customer_id,
    report_period,
    product_type
FROM (
    SELECT
    *,
    ROW_NUMBER() OVER (
    PARTITION BY customer_id, report_period
    ORDER BY order_count DESC, product_type
    ) AS rn
    FROM category_counts
    ) ranked
WHERE rn = 1
    ),

    craftsman_counts AS (
-- Считаем заказы у каждого мастера
SELECT
    c.customer_id,
    DATE_TRUNC('month', f.order_created_at)::DATE AS report_period,
    cr.craftsman_id,
    COUNT(*) AS order_count

FROM dds.fact_orders f
    JOIN dds.dim_customers c
ON f.customer_key = c.customer_key
    JOIN dds.dim_craftsmans cr
    ON f.craftsman_key = cr.craftsman_key

GROUP BY
    c.customer_id,
    DATE_TRUNC('month', f.order_created_at)::DATE,
    cr.craftsman_id
    ),

    top_craftsmans AS (
-- Выбираем наиболее популярного мастера
SELECT
    customer_id,
    report_period,
    craftsman_id
FROM (
    SELECT
    *,
    ROW_NUMBER() OVER (
    PARTITION BY customer_id, report_period
    ORDER BY order_count DESC, craftsman_id
    ) AS rn
    FROM craftsman_counts
    ) ranked
WHERE rn = 1
    )

-- Записываем рассчитанные показатели в CDM
INSERT INTO cdm.customer_report (
    customer_id,
    customer_name,
    customer_address,
    customer_birthday,
    customer_email,
    customer_money,
    platform_money,
    count_order,
    avg_price_order,
    median_time_order,
    top_product_category,
    top_craftsman_id,
    count_order_created,
    count_order_in_progress,
    count_order_delivery,
    count_order_done,
    count_order_not_done,
    report_period,
    report_year,
    report_month
)

SELECT
    m.customer_id,
    m.customer_name,
    m.customer_address,
    m.customer_birthday,
    m.customer_email,
    m.customer_money,
    m.platform_money,
    m.count_order,
    m.avg_price_order,
    m.median_time_order,
    tc.product_type,
    tcr.craftsman_id,
    m.count_order_created,
    m.count_order_in_progress,
    m.count_order_delivery,
    m.count_order_done,
    m.count_order_not_done,
    m.report_period,
    EXTRACT(YEAR FROM m.report_period)::INTEGER,
    EXTRACT(MONTH FROM m.report_period)::INTEGER

FROM monthly_stats m

         LEFT JOIN top_categories tc
                   ON m.customer_id = tc.customer_id
                       AND m.report_period = tc.report_period

         LEFT JOIN top_craftsmans tcr
                   ON m.customer_id = tcr.customer_id
                       AND m.report_period = tcr.report_period

-- При повторной загрузке обновляем существующие строки
    ON CONFLICT (customer_id, report_period)
DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
           customer_address = EXCLUDED.customer_address,
           customer_birthday = EXCLUDED.customer_birthday,
           customer_email = EXCLUDED.customer_email,
           customer_money = EXCLUDED.customer_money,
           platform_money = EXCLUDED.platform_money,
           count_order = EXCLUDED.count_order,
           avg_price_order = EXCLUDED.avg_price_order,
           median_time_order = EXCLUDED.median_time_order,
           top_product_category = EXCLUDED.top_product_category,
           top_craftsman_id = EXCLUDED.top_craftsman_id,
           count_order_created = EXCLUDED.count_order_created,
           count_order_in_progress = EXCLUDED.count_order_in_progress,
           count_order_delivery = EXCLUDED.count_order_delivery,
           count_order_done = EXCLUDED.count_order_done,
           count_order_not_done = EXCLUDED.count_order_not_done,
           report_year = EXCLUDED.report_year,
           report_month = EXCLUDED.report_month;


