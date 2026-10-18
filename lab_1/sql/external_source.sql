-- PostgreSQL читает CSV напрямую: данные не копируются в физические таблицы источника.
-- Сервер csv_files связывает внешние таблицы с обработчиком файлов file_fdw.
-- Явная схема исключает зависимость размещения служебных функций от имени пользователя.
CREATE EXTENSION IF NOT EXISTS file_fdw WITH SCHEMA public;
CREATE SERVER IF NOT EXISTS csv_files FOREIGN DATA WRAPPER file_fdw;
CREATE SCHEMA IF NOT EXISTS external_source;

-- Полный состав и порядок столбцов файла; нужная проекция выбирается при загрузке в STG.
CREATE FOREIGN TABLE IF NOT EXISTS external_source.user_order_log (
 id bigint,
 uniq_id text,
 date_time timestamptz,
 city_id bigint,
 city_name text,
 customer_id bigint,
 first_name text,
 last_name text,
 item_id bigint,
 item_name text,
 quantity integer,
 payment_amount numeric(14,2)
) SERVER csv_files OPTIONS (
 filename '/data/external/user_order_log.csv',
 format 'csv', header 'true', delimiter ',', encoding 'UTF8'
);
CREATE FOREIGN TABLE IF NOT EXISTS external_source.user_activity_log (
 "Id" bigint,
 uniq_id text,
 date_time timestamptz,
 action_id text,
 customer_id bigint,
 quantity integer
) SERVER csv_files OPTIONS (
 filename '/data/external/user_activity_log.csv',
 format 'csv', header 'true', delimiter ',', encoding 'UTF8'
);
CREATE FOREIGN TABLE IF NOT EXISTS external_source.customer_research (
 date_id date,
 category_id bigint,
 geo_id bigint,
 sales_qty integer,
 sales_amt numeric(14,2)
) SERVER csv_files OPTIONS (
 filename '/data/external/customer_research.csv',
 format 'csv', header 'true', delimiter ',', encoding 'UTF8'
);

-- Инициализация завершается ошибкой, если PostgreSQL не может прочитать или разобрать CSV.
SELECT count(*) FROM external_source.user_order_log;
SELECT count(*) FROM external_source.user_activity_log;
SELECT count(*) FROM external_source.customer_research;
