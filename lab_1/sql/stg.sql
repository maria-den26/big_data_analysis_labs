-- Таблицы детальных копий источников, позиций загрузки и общего журнала.
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS etl;
-- Одна позиция на исходную таблицу или коллекцию; сохраняется вместе с порцией.
CREATE TABLE IF NOT EXISTS stg.settings (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 settings_key text NOT NULL UNIQUE,
 settings jsonb NOT NULL
);
-- Счётчики успешно зафиксированных строк по запуску и этапу обработки.
CREATE TABLE IF NOT EXISTS etl.loads (
 batch_id text NOT NULL,stage text NOT NULL,row_count integer NOT NULL,
 completed_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(batch_id,stage)
);

-- Проекции из первого задания 2: только требуемые поля, без технических столбцов.
CREATE TABLE IF NOT EXISTS stg.user_order_log (
 date_time timestamptz NOT NULL,customer_id bigint NOT NULL,
 quantity integer NOT NULL,payment_amount numeric(14,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS stg.user_activity_log (
 date_time timestamptz NOT NULL,customer_id bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS stg.customer_research (
 date_id date NOT NULL,geo_id bigint NOT NULL,
 sales_qty integer NOT NULL,sales_amt numeric(14,2) NOT NULL
);

-- Атрибуты верхнего уровня становятся столбцами; вложенные структуры остаются в JSONB.
-- Даты рождения и основания остаются текстом, поскольку источник MongoDB хранит строки.
CREATE TABLE IF NOT EXISTS stg.clients (
 _id text PRIMARY KEY,name text,phone text,birthday text,email text,login text,
 address jsonb,update_time timestamptz
);
CREATE TABLE IF NOT EXISTS stg.restaurant (
 _id text PRIMARY KEY,name text,phone text,email text,founding_day text,
 menu jsonb,update_time timestamptz,address jsonb
);
CREATE TABLE IF NOT EXISTS stg.orders (
 _id text PRIMARY KEY,restaurant jsonb,order_date timestamptz,client jsonb,
 ordered_dish jsonb,payed_by_bonuses numeric,cost numeric,payment numeric,
 bonus_for_visit numeric,statuses jsonb,final_status text,update_time timestamptz
);

-- Четыре поля по заданию 3: идентификатор, время, имя таблицы и JSON строки.
CREATE TABLE IF NOT EXISTS stg.logs (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 date_time timestamptz NOT NULL DEFAULT clock_timestamp(),
 table_name text NOT NULL,
 record_data jsonb NOT NULL
);
-- При удалении сохраняется предыдущее состояние, при вставке и обновлении — новое.
-- Обновление без изменения значений не создаёт событие журнала.
CREATE OR REPLACE FUNCTION etl.audit_stg_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND to_jsonb(OLD)=to_jsonb(NEW) THEN RETURN NULL; END IF;
 INSERT INTO stg.logs(table_name,record_data)
 VALUES (TG_TABLE_NAME,CASE WHEN TG_OP='DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END);
 RETURN NULL;
END $$;
-- До очистки таблицы журналируем каждую строку, которая будет удалена.
CREATE OR REPLACE FUNCTION etl.audit_stg_truncate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 EXECUTE format('INSERT INTO stg.logs(table_name,record_data) SELECT %L,to_jsonb(t) FROM %I.%I t',
                TG_TABLE_NAME,TG_TABLE_SCHEMA,TG_TABLE_NAME);
 RETURN NULL;
END $$;
-- Триггеры устанавливаются только на таблицы данных; настройки и журнал исключены.
DO $$ DECLARE t record; BEGIN
 FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='stg' AND tablename NOT IN ('logs','settings')
 LOOP
  EXECUTE format('CREATE OR REPLACE TRIGGER audit_changes AFTER INSERT OR UPDATE OR DELETE ON stg.%I FOR EACH ROW EXECUTE FUNCTION etl.audit_stg_row()',t.tablename);
  EXECUTE format('CREATE OR REPLACE TRIGGER audit_clear BEFORE TRUNCATE ON stg.%I FOR EACH STATEMENT EXECUTE FUNCTION etl.audit_stg_truncate()',t.tablename);
 END LOOP;
END $$;
