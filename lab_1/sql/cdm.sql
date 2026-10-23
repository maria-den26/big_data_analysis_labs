CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.customer_report (
                                                   id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

                                                   customer_id BIGINT NOT NULL,
                                                   customer_name VARCHAR(255),
    customer_address VARCHAR(255),
    customer_birthday TIMESTAMP,
    customer_email VARCHAR(255),

    customer_money NUMERIC(12, 2),
    platform_money NUMERIC(12, 2),
    count_order BIGINT,
    avg_price_order NUMERIC(12, 2),
    median_time_order NUMERIC,

    top_product_category VARCHAR(100),
    top_craftsman_id BIGINT,

    count_order_created BIGINT,
    count_order_in_progress BIGINT,
    count_order_delivery BIGINT,
    count_order_done BIGINT,
    count_order_not_done BIGINT,

    report_period DATE NOT NULL,
    report_year INTEGER NOT NULL,
    report_month INTEGER NOT NULL,

    UNIQUE (customer_id, report_period)
    );


