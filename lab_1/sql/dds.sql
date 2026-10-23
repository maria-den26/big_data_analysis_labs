
CREATE SCHEMA IF NOT EXISTS dds;

-- 1. Покупатели
CREATE TABLE IF NOT EXISTS dds.dim_customers (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                 customer_id BIGINT NOT NULL UNIQUE,
                                                 customer_name VARCHAR(255),
    customer_address VARCHAR(255),
    customer_birthday TIMESTAMP,
    customer_email VARCHAR(255)
    );

-- 2. Мастера
CREATE TABLE IF NOT EXISTS dds.dim_craftsmans (
                                                  craftsman_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                  craftsman_id BIGINT NOT NULL UNIQUE,
                                                  craftsman_name VARCHAR(255),
    craftsman_address VARCHAR(255),
    craftsman_birthday TIMESTAMP,
    craftsman_email VARCHAR(255)
    );

-- 3. Товары
CREATE TABLE IF NOT EXISTS dds.dim_products (
                                                product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                product_id BIGINT NOT NULL UNIQUE,
                                                product_name VARCHAR(255),
    product_description TEXT,
    product_type VARCHAR(100)
    );

-- 4. Календарь
CREATE TABLE IF NOT EXISTS dds.dim_dates (
                                             date_key DATE PRIMARY KEY,
                                             year INTEGER NOT NULL,
                                             quarter INTEGER NOT NULL,
                                             month INTEGER NOT NULL,
                                             day INTEGER NOT NULL
);

-- 5. Таблица фактов
CREATE TABLE IF NOT EXISTS dds.fact_orders (
                                               order_id BIGINT PRIMARY KEY,

                                               customer_key BIGINT
                                               REFERENCES dds.dim_customers(customer_key),

    craftsman_key BIGINT
    REFERENCES dds.dim_craftsmans(craftsman_key),

    product_key BIGINT
    REFERENCES dds.dim_products(product_key),

    created_date_key DATE
    REFERENCES dds.dim_dates(date_key),

    completed_date_key DATE
    REFERENCES dds.dim_dates(date_key),

    order_created_at TIMESTAMP,
    order_completed_at TIMESTAMP,
    order_status VARCHAR(100),
    product_price NUMERIC(10, 2)
    );



