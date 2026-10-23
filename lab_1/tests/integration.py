"""Интеграционные проверки в отдельных временных базах PostgreSQL и MongoDB."""
import csv
import importlib.util
import io
import os
import shlex
import unittest
from contextlib import redirect_stdout
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from pymongo import MongoClient

import dwh_lib as etl
from stg_settings import get_setting, save_setting

# Подключаем функции инициализации без запуска создания пользователя Airflow.
spec = importlib.util.spec_from_file_location('lab_setup', '/opt/airflow/scripts/setup.py')
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)

# Проверка согласованности использует тот же изолированный контур, что и остальные тесты.
audit_spec = importlib.util.spec_from_file_location('lab_audit', '/opt/airflow/scripts/check_consistency.py')
audit = importlib.util.module_from_spec(audit_spec)
audit_spec.loader.exec_module(audit)


def pipeline(batch=None):
    """Выполняет обе загрузки и проверку с общим идентификатором тестового запуска."""
    batch = batch or 'test_' + uuid4().hex
    counts = (etl.load_pg(batch), etl.load_mongo(batch))
    etl.validate_stg(batch)
    return counts


def snapshot():
    """Снимает упорядоченное состояние данных, журнала и позиций для точного сравнения."""
    with etl.connect() as conn:
        return {table: conn.execute(sql.SQL('SELECT to_jsonb(t)::text FROM {} t ORDER BY to_jsonb(t)::text').format(
            sql.Identifier('stg', table))).fetchall() for table in (*etl.DATA_TABLES, 'logs', 'settings')}


def setting(key):
    """Читает сохранённую позицию выбранного источника."""
    with etl.connect() as conn:
        return get_setting(conn, key)


def count(table):
    """Возвращает количество строк заданной таблицы STG."""
    with etl.connect() as conn:
        return conn.execute(sql.SQL('SELECT count(*) FROM {}').format(sql.Identifier('stg', table))).fetchone()[0]


def csv_source(table, header, rows):
    """Подменяет CSV тестовой внешней таблицы контролируемым содержимым."""
    # Настоящий file_fdw читает вывод команды; временные файлы на диске не создаются.
    stream = io.StringIO()
    writer = csv.writer(stream, lineterminator='\n')
    writer.writerow(header)
    writer.writerows(rows)
    command = 'printf %s ' + shlex.quote(stream.getvalue())
    with etl.connect('SOURCE_PG_DSN') as conn:
        options = conn.execute('''SELECT ftoptions FROM pg_foreign_table
            WHERE ftrelid=%s::regclass''', ('external_source.' + table,)).fetchone()[0]
        previous = 'program' if any(o.startswith('program=') for o in options) else 'filename'
        conn.execute(sql.SQL('ALTER FOREIGN TABLE {} OPTIONS (DROP {}, ADD program {})').format(
            sql.Identifier('external_source', table), sql.Identifier(previous), sql.Literal(command)))


def add_customers(*ids):
    """Добавляет клиентов с заданными ключами в изолированную базу источника."""
    with etl.connect('SOURCE_PG_DSN') as conn:
        for identifier in ids:
            conn.execute('''INSERT INTO source.craft_market_customers(customer_id,customer_name,customer_email)
                VALUES (%s,%s,%s)''', (identifier, f'Customer {identifier}', f'{identifier}@example.test'))


class IncrementalTests(unittest.TestCase):
    """Проверки структуры, порций, повторных запусков и восстановления после сбоев."""
    @classmethod
    def setUpClass(cls):
        """Создаёт отдельные базы и переключает окружение тестов на них."""
        cls.original = {key: os.environ.get(key) for key in
                        ('DWH_DSN', 'SOURCE_PG_DSN', 'SOURCE_MONGO_DB', 'STG_BATCH_LIMIT')}
        cls.dbname = 'lab_test_' + uuid4().hex
        cls.admin = {}
        cls.addClassCleanup(cls.cleanup)
        for key in ('SOURCE_PG_DSN', 'DWH_DSN'):
            original = conninfo_to_dict(os.environ[key])
            conn = psycopg.connect(make_conninfo(**original), autocommit=True)
            cls.admin[key] = conn
            conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.dbname)))
            original['dbname'] = cls.dbname
            os.environ[key] = make_conninfo(**original)
        os.environ['SOURCE_MONGO_DB'] = cls.dbname
        os.environ['STG_BATCH_LIMIT'] = '2'

    @classmethod
    def cleanup(cls):
        """Удаляет временные базы и восстанавливает переменные окружения."""
        with MongoClient(os.environ['SOURCE_MONGO_URI']) as client:
            client.drop_database(cls.dbname)
        for conn in cls.admin.values():
            conn.execute(sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(cls.dbname)))
            conn.close()
        for key, value in cls.original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def setUp(self):
        """Создаёт начальную структуру и данные перед каждым независимым сценарием."""
        # Перед очисткой проверяем, что подключение ведёт в созданную тестовую базу.
        for key, schemas in [('SOURCE_PG_DSN', ('source', 'external_source')),
                             ('DWH_DSN', ('stg', 'etl'))]:
            with etl.connect(key) as conn:
                self.assertEqual(conn.info.dbname, self.dbname)
                for schema in schemas:
                    conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
                if key == 'SOURCE_PG_DSN':
                    conn.execute('DROP TABLE IF EXISTS public.lab_initialized')
        with MongoClient(os.environ['SOURCE_MONGO_URI']) as client:
            client.drop_database(self.dbname)
        setup.initialize_databases()

    def test_01_fresh_start_structure_and_values(self):
        """Проверяет создание с нуля, состав источников и точность переноса значений."""
        self.assertEqual(pipeline(), (36, 4))
        self.assertEqual(sum(count(t) for t in etl.DATA_TABLES), 40)
        self.assertEqual(count('logs'), 40)
        self.assertEqual(count('settings'), 12)
        with etl.connect('SOURCE_PG_DSN') as source, etl.connect() as dest:
            self.assertEqual({r[0] for r in dest.execute("SELECT tablename FROM pg_tables WHERE schemaname='stg'")},
                             set(etl.DATA_TABLES) | {'logs', 'settings'})
            self.assertEqual({r[0] for r in source.execute("SELECT tablename FROM pg_tables WHERE schemaname='source'")}, set(etl.CRAFT_KEYS))
            self.assertEqual(source.execute("SELECT count(*) FROM information_schema.foreign_tables WHERE foreign_table_schema='external_source'").fetchone()[0], 3)
            self.assertEqual(source.execute("SELECT count(*) FROM information_schema.triggers WHERE trigger_schema='source'").fetchone()[0], 0)
            metadata = '''SELECT column_name,data_type,is_nullable,character_maximum_length,
                numeric_precision,numeric_scale,datetime_precision
                FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position'''
            for table in etl.CRAFT_KEYS:
                self.assertEqual(source.execute(metadata, ('source', table)).fetchall(), dest.execute(metadata, ('stg', table)).fetchall())
                query = sql.SQL('SELECT to_jsonb(t)::text FROM {} t ORDER BY to_jsonb(t)::text')
                self.assertEqual(source.execute(query.format(sql.Identifier('source', table))).fetchall(),
                                 dest.execute(query.format(sql.Identifier('stg', table))).fetchall())
            for table, columns in etl.EXTERNAL_COLUMNS.items():
                self.assertEqual(etl.column_names(dest, 'stg', table), columns)
                query = sql.SQL('SELECT {} FROM {} ORDER BY {}')
                cols = etl.identifiers(columns)
                self.assertEqual(source.execute(query.format(cols, sql.Identifier('external_source', table), cols)).fetchall(),
                                 dest.execute(query.format(cols, sql.Identifier('stg', table), cols)).fetchall())
        with MongoClient(os.environ['SOURCE_MONGO_URI'], tz_aware=True) as client, etl.connect() as conn:
            db = etl.mongo_database(client)
            self.assertEqual(set(db.list_collection_names()), set(etl.MONGO_COLUMNS))
            for table in etl.MONGO_COLUMNS:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql.SQL('SELECT * FROM {}').format(sql.Identifier('stg', table)))
                    rows = {row['_id']: row for row in cur.fetchall()}
                for document in db[table].find():
                    for key, value in document.items():
                        self.assertEqual(rows[document['_id']][key], value)

    def test_02_batches_repeat_and_setup_preserve_progress(self):
        """Проверяет размер порций и сохранность данных и позиций при повторном запуске."""
        sizes = []
        original = etl.write_rows
        def observe(conn, table, columns, rows, keys=()):
            """Запоминает фактический размер порции, сохраняя обычную запись строк."""
            sizes.append((table, len(rows)))
            return original(conn, table, columns, rows, keys)
        with patch.object(etl, 'write_rows', side_effect=observe):
            pipeline()
        self.assertTrue(all(size <= 2 for _, size in sizes))
        self.assertEqual([size for table, size in sizes if table == 'craft_market_wide'], [2, 1])
        before = snapshot()
        setup.initialize_databases()
        self.assertEqual(pipeline(), (0, 0))
        self.assertEqual(before, snapshot())

    def test_03_new_pg_ids_only(self):
        """Подтверждает доставку новых ключей и согласованное игнорирование старых изменений."""
        pipeline()
        before = count('logs')
        add_customers(101, 102, 103)
        self.assertEqual(pipeline(), (3, 0))
        self.assertEqual(count('logs'), before + 3)
        self.assertEqual(setting('source.craft_market_customers'), {'last_loaded_key': [103]})
        # Изменения и удаления пройденных записей не входят в согласованный режим загрузки новых строк.
        with etl.connect('SOURCE_PG_DSN') as conn:
            conn.execute("UPDATE source.craft_market_customers SET customer_name='changed' WHERE customer_id=101")
            conn.execute('DELETE FROM source.craft_market_customers WHERE customer_id=102')
        self.assertEqual(pipeline(), (0, 0))
        with etl.connect() as conn:
            self.assertEqual(conn.execute('SELECT customer_name FROM stg.craft_market_customers WHERE customer_id=101').fetchone()[0], 'Customer 101')
            self.assertTrue(conn.execute('SELECT 1 FROM stg.craft_market_customers WHERE customer_id=102').fetchone())

    def test_04_pg_failure_resumes_after_committed_batch(self):
        """Проверяет откат ошибочной порции PostgreSQL и продолжение после успешной."""
        pipeline()
        add_customers(101, 102, 103, 104)
        before = count('logs')
        original = etl.save_setting
        def fail_checkpoint(conn, key, state):
            """Имитирует сбой при сохранении позиции второй порции клиентов."""
            if key == 'source.craft_market_customers' and state['last_loaded_key'] == [104]:
                raise RuntimeError('simulated checkpoint failure')
            return original(conn, key, state)
        with patch.object(etl, 'save_setting', side_effect=fail_checkpoint):
            with self.assertRaisesRegex(RuntimeError, 'checkpoint failure'):
                etl.load_pg('retry_pg')
        self.assertEqual(setting('source.craft_market_customers'), {'last_loaded_key': [102]})
        self.assertEqual(count('logs'), before + 2)
        with etl.connect() as conn:
            self.assertIsNone(conn.execute('SELECT 1 FROM stg.craft_market_customers WHERE customer_id=103').fetchone())
        self.assertEqual(etl.load_pg('retry_pg'), 2)
        self.assertEqual(count('logs'), before + 4)
        with etl.connect() as conn:
            self.assertEqual(conn.execute("SELECT row_count FROM etl.loads WHERE batch_id='retry_pg' AND stage='pg_stg'").fetchone()[0], 4)

    def test_05_craftsman_key_and_parent_batches(self):
        """Проверяет ключ мастера и загрузку всех родительских порций до дочерних."""
        with etl.connect('SOURCE_PG_DSN') as conn:
            for craftsman in (2, 3, 4):
                conn.execute('''INSERT INTO source.craft_market_masters_products
                    SELECT %s,craftsman_name,craftsman_address,craftsman_birthday,craftsman_email,
                    product_id,product_name,product_description,product_type,product_price
                    FROM source.craft_market_masters_products WHERE craftsman_id=1''', (craftsman,))
            conn.execute('''INSERT INTO source.craft_market_orders_customers
                SELECT 100,4,999,order_created_date,order_completion_date,order_status,
                customer_id,customer_name,customer_address,customer_birthday,customer_email
                FROM source.craft_market_orders_customers WHERE order_id=1''')
        pipeline()
        self.assertEqual(setting('source.craft_market_masters_products'), {'last_loaded_key': [4]})
        self.assertEqual(count('craft_market_masters_products'), 4)
        self.assertEqual(count('craft_market_orders_customers'), 3)

    def test_06_csv_duplicates_and_checkpoint_rollback(self):
        """Проверяет дубликаты проекции CSV, откат позиции и доставку следующих записей."""
        header = ('Id', 'uniq_id', 'date_time', 'action_id', 'customer_id', 'quantity')
        rows = [(i, f'event-{i}', '2024-04-01T12:00:00Z', 'view', 1, 1) for i in range(1, 6)]
        csv_source('user_activity_log', header, rows)
        original = etl.save_setting
        def fail_checkpoint(conn, key, state):
            """Имитирует сбой записи позиции второй порции внешнего журнала."""
            if key == 'external_source.user_activity_log' and state['last_loaded_key'] == [4]:
                raise RuntimeError('csv checkpoint failure')
            return original(conn, key, state)
        with patch.object(etl, 'save_setting', side_effect=fail_checkpoint):
            with self.assertRaisesRegex(RuntimeError, 'csv checkpoint'):
                etl.load_pg('retry_csv')
        self.assertEqual(count('user_activity_log'), 2)
        self.assertEqual(setting('external_source.user_activity_log'), {'last_loaded_key': [2]})
        etl.load_pg('retry_csv')
        self.assertEqual(count('user_activity_log'), 5)
        before = snapshot()
        self.assertEqual(etl.load_pg('empty_csv'), 0)
        self.assertEqual(before, snapshot())
        csv_source('user_activity_log', header, rows + [(6, 'event-6', '2024-04-01T12:00:00Z', 'view', 1, 1)])
        self.assertEqual(etl.load_pg('append_csv'), 1)
        self.assertEqual(count('user_activity_log'), 6)

    def test_07_research_composite_cursor_and_ambiguous_keys(self):
        """Проверяет продолжение по тройному ключу и отклонение неоднозначных ключей."""
        pipeline()
        self.assertEqual(setting('external_source.customer_research'), {'last_loaded_key': ['2024-04-01', 2, 78]})
        header = ('date_id', 'category_id', 'geo_id', 'sales_qty', 'sales_amt')
        csv_source('customer_research', header, [('2024-04-02', 1, geo, 1, 10) for geo in (77, 78, 79)])
        self.assertEqual(etl.load_pg('research_append'), 3)
        self.assertEqual(count('customer_research'), 6)
        before = snapshot()
        csv_source('customer_research', header, [('2024-04-03', 1, 77, 1, 10)] * 2)
        with self.assertRaisesRegex(ValueError, 'unique and non-null'):
            etl.load_pg('bad_keys')
        self.assertEqual(before, snapshot())

    def test_08_mongo_timestamp_ties_and_updates(self):
        """Проверяет одинаковое время нескольких документов и обновление с новым временем."""
        pipeline()
        timestamp = datetime(2030, 1, 1, tzinfo=timezone.utc)
        with MongoClient(os.environ['SOURCE_MONGO_URI']) as client:
            collection = etl.mongo_database(client).clients
            collection.insert_many([{'_id': f'new-{i}', 'name': 'New', 'update_time': timestamp} for i in range(5)])
        self.assertEqual(etl.load_mongo('mongo_ties'), 5)
        self.assertEqual(setting('mongo.clients')['last_loaded_id'], 'new-4')
        before = count('logs')
        with MongoClient(os.environ['SOURCE_MONGO_URI']) as client:
            etl.mongo_database(client).clients.update_one({'_id': 'new-0'}, {'$set': {
                'name': 'Updated', 'update_time': timestamp + timedelta(seconds=1), 'address': {'city': 'СПб'}}})
        self.assertEqual(etl.load_mongo('mongo_update'), 1)
        self.assertEqual(count('logs'), before + 1)
        with etl.connect() as conn:
            self.assertEqual(conn.execute("SELECT name,address FROM stg.clients WHERE _id='new-0'").fetchone(), ('Updated', {'city': 'СПб'}))
        self.assertEqual(etl.load_mongo('mongo_repeat'), 0)

    def test_09_mongo_bad_batch_retains_previous_progress(self):
        """Проверяет восстановление после неизвестного атрибута в порции MongoDB."""
        pipeline()
        timestamp = datetime(2030, 1, 1, tzinfo=timezone.utc)
        with MongoClient(os.environ['SOURCE_MONGO_URI']) as client:
            collection = etl.mongo_database(client).clients
            collection.insert_many([{'_id': f'new-{i}', 'name': 'New', 'update_time': timestamp} for i in range(4)])
            collection.update_one({'_id': 'new-3'}, {'$set': {'unexpected': True}})
        before = count('logs')
        with self.assertRaisesRegex(ValueError, 'Unmapped MongoDB'):
            etl.load_mongo('mongo_retry')
        self.assertEqual(setting('mongo.clients')['last_loaded_id'], 'new-1')
        self.assertEqual(count('logs'), before + 2)
        with MongoClient(os.environ['SOURCE_MONGO_URI']) as client:
            etl.mongo_database(client).clients.update_one({'_id': 'new-3'}, {'$unset': {'unexpected': ''}})
        self.assertEqual(etl.load_mongo('mongo_retry'), 2)
        self.assertEqual(count('logs'), before + 4)

    def test_10_log_format_settings_excluded_and_transaction(self):
        """Проверяет формат журнала, исключение настроек из аудита и общий откат."""
        pipeline()
        before = snapshot()
        with etl.connect() as conn:
            self.assertEqual(etl.column_names(conn, 'stg', 'logs'), ('id', 'date_time', 'table_name', 'record_data'))
            boundary = conn.execute('SELECT max(id) FROM stg.logs').fetchone()[0]
            conn.execute("INSERT INTO stg.clients(_id,name) VALUES ('audit-test','before')")
            conn.execute("UPDATE stg.clients SET name='after' WHERE _id='audit-test'")
            conn.execute("UPDATE stg.clients SET name=name WHERE _id='audit-test'")
            conn.execute("DELETE FROM stg.clients WHERE _id='audit-test'")
            save_setting(conn, 'test.setting', {'last_loaded_key': [1]})
            events = conn.execute('SELECT table_name,record_data FROM stg.logs WHERE id>%s ORDER BY id', (boundary,)).fetchall()
            self.assertEqual([event[1]['name'] for event in events], ['before', 'after', 'after'])
            self.assertTrue(all(event[0] == 'clients' for event in events))
            self.assertNotIn('new', events[0][1])
            conn.rollback()
        self.assertEqual(before, snapshot())

    def test_11_missing_csv_and_invalid_limit_do_not_advance_cursor(self):
        """Подтверждает сохранение позиций при недоступном файле и неверном размере порции."""
        pipeline()
        before = snapshot()
        with etl.connect('SOURCE_PG_DSN') as conn:
            conn.execute("ALTER FOREIGN TABLE external_source.user_order_log OPTIONS (SET filename '/data/external/missing_test.csv')")
        with self.assertRaises(psycopg.errors.UndefinedFile):
            etl.load_pg('missing_csv')
        self.assertEqual(before, snapshot())
        with patch.dict(os.environ, {'STG_BATCH_LIMIT': '0'}):
            with self.assertRaisesRegex(ValueError, 'positive'):
                etl.load_mongo('invalid_limit')
        self.assertEqual(before, snapshot())

    def test_13_craft_types_and_constraints_match_requested_schema(self):
        """Сверяет типы и ограничения обеих схем с заданной моделью, независимо от загрузчика."""
        primary_keys = {
            'craft_market_wide': 'id',
            'craft_market_craftsmans': 'craftsman_id',
            'craft_market_customers': 'customer_id',
            'craft_market_orders': 'order_id',
            'craft_market_masters_products': 'craftsman_id',
            'craft_market_orders_customers': 'order_id',
        }
        foreign_keys = {
            'craft_market_orders': {
                'FOREIGN KEY (craftsman_id) REFERENCES {schema}.craft_market_craftsmans(craftsman_id)',
                'FOREIGN KEY (customer_id) REFERENCES {schema}.craft_market_customers(customer_id)',
            },
            'craft_market_orders_customers': {
                'FOREIGN KEY (craftsman_id) REFERENCES {schema}.craft_market_masters_products(craftsman_id)',
            },
        }
        for connection, schema in [('SOURCE_PG_DSN', 'source'), ('DWH_DSN', 'stg')]:
            with etl.connect(connection) as conn:
                # Полные имена схем в описании связей не должны зависеть от имени пользователя.
                conn.execute('SET LOCAL search_path=pg_catalog')
                for table, primary in primary_keys.items():
                    columns = conn.execute('''SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull
                        FROM pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0
                        AND NOT a.attisdropped ORDER BY a.attnum''', (f'{schema}.{table}',)).fetchall()
                    for name, actual_type, required in columns:
                        if name == primary:
                            expected_type = 'bigint'
                        elif name.endswith('_id'):
                            expected_type = 'integer'
                        elif name.endswith(('_birthday', '_date')):
                            expected_type = 'timestamp without time zone'
                        elif name == 'product_price':
                            expected_type = 'numeric(10,2)'
                        elif name == 'product_description':
                            expected_type = 'text'
                        elif name in ('product_type', 'order_status'):
                            expected_type = 'character varying(100)'
                        else:
                            expected_type = 'character varying(255)'
                        self.assertEqual(actual_type, expected_type, (schema, table, name))
                        self.assertEqual(required, name == primary, (schema, table, name))
                    actual_constraints = set(conn.execute('''SELECT contype,pg_get_constraintdef(oid)
                        FROM pg_constraint WHERE conrelid=%s::regclass''', (f'{schema}.{table}',)).fetchall())
                    expected_constraints = {('p', f'PRIMARY KEY ({primary})')} | {
                        ('f', definition.format(schema=schema)) for definition in foreign_keys.get(table, set())}
                    self.assertEqual(actual_constraints, expected_constraints, (schema, table))

    def test_14_nullable_values_timestamp_precision_and_type_limits(self):
        """Проверяет перенос NULL и точного времени, границы типов и новые ограничения ключей."""
        pipeline()
        timestamp = datetime(2026, 10, 16, 15, 24, 31, 123456)
        with etl.connect('SOURCE_PG_DSN') as conn:
            for table, keys in etl.CRAFT_KEYS.items():
                conn.execute(sql.SQL('INSERT INTO {} ({}) VALUES (100)').format(
                    sql.Identifier('source', table), sql.Identifier(keys[0])))
            conn.execute('''UPDATE source.craft_market_wide SET craftsman_birthday=%s,
                customer_birthday=%s,order_created_date=%s,order_completion_date=%s,
                product_price=-0.01,order_id=1 WHERE id=100''', (timestamp,) * 4)
            conn.execute('''INSERT INTO source.craft_market_wide
                (id,craftsman_name,product_type,order_status,product_price,product_id,order_id)
                VALUES (101,%s,%s,%s,99999999.99,2147483647,1)''', ('Я' * 255, 'Т' * 100, 'С' * 100))
        self.assertEqual(pipeline(), (7, 0))
        with etl.connect() as conn:
            self.assertEqual(conn.execute('''SELECT craftsman_birthday,customer_birthday,
                order_created_date,order_completion_date,product_price,customer_name
                FROM stg.craft_market_wide WHERE id=100''').fetchone(),
                (timestamp, timestamp, timestamp, timestamp, Decimal('-0.01'), None))
            self.assertEqual(conn.execute('''SELECT product_price,product_id,char_length(craftsman_name)
                FROM stg.craft_market_wide WHERE id=101''').fetchone(),
                (Decimal('99999999.99'), 2147483647, 255))
        # Проверяем ограничения отдельно в источнике и копии STG, откатывая каждую ошибочную вставку.
        for connection, schema in [('SOURCE_PG_DSN', 'source'), ('DWH_DSN', 'stg')]:
            with etl.connect(connection) as conn:
                invalid_values = [
                    ('craftsman_name', 'Я' * 256, psycopg.errors.StringDataRightTruncation),
                    ('product_type', 'Т' * 101, psycopg.errors.StringDataRightTruncation),
                    ('product_price', Decimal('100000000.00'), psycopg.errors.NumericValueOutOfRange),
                    ('product_id', 2147483648, psycopg.errors.NumericValueOutOfRange),
                ]
                for column, value, error in invalid_values:
                    with self.assertRaises(error):
                        with conn.transaction():
                            conn.execute(sql.SQL('INSERT INTO {} (id,{}) VALUES (102,%s)').format(
                                sql.Identifier(schema, 'craft_market_wide'), sql.Identifier(column)), (value,))
                with self.assertRaises(psycopg.errors.UniqueViolation):
                    with conn.transaction():
                        conn.execute(sql.SQL('INSERT INTO {} (craftsman_id,product_id) VALUES (1,999)').format(
                            sql.Identifier(schema, 'craft_market_masters_products')))
                with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                    with conn.transaction():
                        conn.execute(sql.SQL('INSERT INTO {} (order_id,craftsman_id) VALUES (102,999)').format(
                            sql.Identifier(schema, 'craft_market_orders_customers')))

    def test_15_consistency_audit_detects_extra_objects(self):
        """Проверяет совпадение с чистой установкой и обнаружение посторонней таблицы."""
        pipeline()
        before = snapshot()
        with redirect_stdout(io.StringIO()):
            self.assertTrue(audit.check_consistency())
        self.assertEqual(before, snapshot())
        with etl.connect() as conn:
            conn.execute('CREATE TABLE stg.unused_test_table (id integer)')
        with redirect_stdout(io.StringIO()):
            self.assertFalse(audit.check_consistency())
        with etl.connect() as conn:
            conn.execute('DROP TABLE stg.unused_test_table')
        self.assertEqual(before, snapshot())

    def test_12_source_lock_and_checkpoint_required(self):
        """Проверяет блокировку конкурирующей загрузки и защиту от потери позиции CSV."""
        pipeline()
        before = snapshot()
        with etl.connect() as owner:
            etl.start_loader(owner, 31001)
            with self.assertRaisesRegex(RuntimeError, 'already running'):
                etl.load_pg('concurrent')
        self.assertEqual(before, snapshot())
        with etl.connect() as conn:
            conn.execute("DELETE FROM stg.settings WHERE settings_key='external_source.user_order_log'")
        with self.assertRaisesRegex(ValueError, 'Non-empty STG without checkpoint'):
            etl.load_pg('missing_checkpoint')
        self.assertEqual(count('user_order_log'), 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
