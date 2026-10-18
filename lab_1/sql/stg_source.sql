-- Копии шести таблиц магазина: типы, ключи и допустимость NULL совпадают с источником.
CREATE SCHEMA IF NOT EXISTS stg;

-- Широкая таблица содержит заказ и сведения о мастере, товаре и покупателе.
CREATE TABLE IF NOT EXISTS stg.craft_market_wide (
 id bigint PRIMARY KEY,
 craftsman_id integer,
 craftsman_name varchar(255),
 craftsman_address varchar(255),
 craftsman_birthday timestamp,
 craftsman_email varchar(255),
 product_id integer,
 product_name varchar(255),
 product_description text,
 product_type varchar(100),
 product_price numeric(10,2),
 order_id integer,
 order_created_date timestamp,
 order_completion_date timestamp,
 order_status varchar(100),
 customer_id integer,
 customer_name varchar(255),
 customer_address varchar(255),
 customer_birthday timestamp,
 customer_email varchar(255)
);

-- Первичный ключ мастера допускает только одну строку с товаром на craftsman_id.
CREATE TABLE IF NOT EXISTS stg.craft_market_masters_products (
 craftsman_id bigint PRIMARY KEY,
 craftsman_name varchar(255),
 craftsman_address varchar(255),
 craftsman_birthday timestamp,
 craftsman_email varchar(255),
 product_id integer,
 product_name varchar(255),
 product_description text,
 product_type varchar(100),
 product_price numeric(10,2)
);

-- Заказ ссылается на мастера; product_id не входит во внешний ключ.
CREATE TABLE IF NOT EXISTS stg.craft_market_orders_customers (
 order_id bigint PRIMARY KEY,
 craftsman_id integer,
 product_id integer,
 order_created_date timestamp,
 order_completion_date timestamp,
 order_status varchar(100),
 customer_id integer,
 customer_name varchar(255),
 customer_address varchar(255),
 customer_birthday timestamp,
 customer_email varchar(255),
 FOREIGN KEY (craftsman_id) REFERENCES stg.craft_market_masters_products(craftsman_id)
);

-- Справочник мастеров для варианта с отдельной таблицей заказов.
CREATE TABLE IF NOT EXISTS stg.craft_market_craftsmans (
 craftsman_id bigint PRIMARY KEY,
 craftsman_name varchar(255),
 craftsman_address varchar(255),
 craftsman_birthday timestamp,
 craftsman_email varchar(255)
);

-- Справочник покупателей; неключевые атрибуты допускают NULL.
CREATE TABLE IF NOT EXISTS stg.craft_market_customers (
 customer_id bigint PRIMARY KEY,
 customer_name varchar(255),
 customer_address varchar(255),
 customer_birthday timestamp,
 customer_email varchar(255)
);

-- Идентификатор заказа — BIGINT; ссылки на мастера и покупателя — INTEGER по заданию.
CREATE TABLE IF NOT EXISTS stg.craft_market_orders (
 order_id bigint PRIMARY KEY,
 product_id integer,
 craftsman_id integer,
 customer_id integer,
 order_created_date timestamp,
 order_completion_date timestamp,
 order_status varchar(100),
 product_name varchar(255),
 product_description text,
 product_type varchar(100),
 product_price numeric(10,2),
 FOREIGN KEY (craftsman_id) REFERENCES stg.craft_market_craftsmans(craftsman_id),
 FOREIGN KEY (customer_id) REFERENCES stg.craft_market_customers(customer_id)
);
