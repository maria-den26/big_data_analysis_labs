"""Инкрементальная загрузка порциями с общей транзакцией для данных, журнала и позиции."""
import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from pymongo import MongoClient
from bson import json_util

from stg_settings import get_setting, save_setting

log = logging.getLogger(__name__)
SQL_DIR = Path('/opt/airflow/sql')
# Все порции родительских таблиц загружаются раньше дочерних для соблюдения связей.
CRAFT_KEYS = {
    'craft_market_craftsmans': ('craftsman_id',),
    'craft_market_customers': ('customer_id',),
    'craft_market_masters_products': ('craftsman_id',),
    'craft_market_wide': ('id',),
    'craft_market_orders_customers': ('order_id',),
    'craft_market_orders': ('order_id',),
}
EXTERNAL_COLUMNS = {
    'user_order_log': ('date_time', 'customer_id', 'quantity', 'payment_amount'),
    'user_activity_log': ('date_time', 'customer_id'),
    'customer_research': ('date_id', 'geo_id', 'sales_qty', 'sales_amt'),
}
# Ключи источников задают границы порций и не добавляются в проекции таблиц STG.
EXTERNAL_KEYS = {
    'user_order_log': ('id',),
    'user_activity_log': ('Id',),
    'customer_research': ('date_id', 'category_id', 'geo_id'),
}
MONGO_COLUMNS = {
    'clients': ('_id', 'name', 'phone', 'birthday', 'email', 'login', 'address', 'update_time'),
    'restaurant': ('_id', 'name', 'phone', 'email', 'founding_day', 'menu', 'update_time', 'address'),
    'orders': ('_id', 'restaurant', 'order_date', 'client', 'ordered_dish', 'payed_by_bonuses',
               'cost', 'payment', 'bonus_for_visit', 'statuses', 'final_status', 'update_time'),
}
# Вложенные значения сохраняются целиком; денежные суммы преобразуются без округления.
JSON_COLUMNS = {'address', 'menu', 'restaurant', 'client', 'ordered_dish', 'statuses'}
MONEY_COLUMNS = {'payed_by_bonuses', 'cost', 'payment', 'bonus_for_visit'}
DATA_TABLES = (*CRAFT_KEYS, *EXTERNAL_COLUMNS, *MONGO_COLUMNS)


def connect(name='DWH_DSN'):
    """Открывает подключение из переменной окружения с единым часовым поясом UTC."""
    return psycopg.connect(os.environ[name], options='-c timezone=UTC')


def mongo_database(client):
    """Выбирает рабочую базу MongoDB или отдельную базу, заданную тестами."""
    return client[os.environ.get('SOURCE_MONGO_DB', 'restaurant_source')]


def execute_file(conn, name):
    """Выполняет SQL-файл внутри транзакции переданного подключения."""
    conn.execute((SQL_DIR / name).read_text(encoding='utf-8'))


def batch_limit():
    """Читает и проверяет максимальное число строк в одной порции."""
    limit = int(os.environ.get('STG_BATCH_LIMIT', '2'))
    if limit < 1:
        raise ValueError('STG_BATCH_LIMIT must be positive')
    return limit


def column_names(conn, schema, table):
    """Возвращает столбцы в исходном порядке для проверки структуры и копирования."""
    return tuple(r[0] for r in conn.execute('''SELECT column_name FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position''', (schema, table)))


def identifiers(columns, alias=None):
    """Безопасно формирует список SQL-идентификаторов с необязательным псевдонимом."""
    return sql.SQL(',').join(sql.Identifier(alias, c) if alias else sql.Identifier(c) for c in columns)


def start_loader(conn, lock_id):
    """Запрещает параллельную работу двух загрузчиков одной группы источников."""
    # Сессионная блокировка действует между транзакциями и снимается при закрытии подключения.
    acquired = conn.execute('SELECT pg_try_advisory_lock(%s)', (lock_id,)).fetchone()[0]
    conn.commit()
    if not acquired:
        raise RuntimeError('Another loader for this source is already running')


def record_batch(conn, batch, stage, count):
    """Прибавляет число обработанных строк к итогу запуска в транзакции порции."""
    conn.execute('''INSERT INTO etl.loads(batch_id,stage,row_count) VALUES (%s,%s,%s)
        ON CONFLICT(batch_id,stage) DO UPDATE
        SET row_count=etl.loads.row_count+EXCLUDED.row_count,completed_at=now()''',
        (batch, stage, count))


def write_rows(conn, table, columns, rows, keys=()):
    """Записывает порцию; при наличии ключа обновляет только изменившиеся значения."""
    query = sql.SQL('INSERT INTO {} AS target ({}) VALUES ({})').format(
        sql.Identifier('stg', table), identifiers(columns),
        sql.SQL(',').join(sql.Placeholder() for _ in columns))
    if keys:
        # Проверка различий предотвращает лишние обновления и события общего журнала.
        values = tuple(c for c in columns if c not in keys)
        query += sql.SQL(''' ON CONFLICT ({}) DO UPDATE SET {}
            WHERE ROW({}) IS DISTINCT FROM ROW({})''').format(
            identifiers(keys),
            sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(c), sql.Identifier(c)) for c in values),
            identifiers(values, 'target'), identifiers(values, 'excluded'))
    with conn.cursor() as cur:
        cur.executemany(query, rows)


def checkpoint_key(values):
    """Представляет составной ключ в JSON, сохраняя даты в строковом формате ISO."""
    return [v.isoformat() if isinstance(v, (date, datetime)) else v for v in values]


def check_external_keys(source, table, keys):
    """Проверяет, что ключи CSV позволяют однозначно продолжить загрузку."""
    # Внешняя таблица не обеспечивает первичный ключ: повторы и пустые значения отклоняем явно.
    invalid = source.execute(sql.SQL('''SELECT {} FROM {} GROUP BY {}
        HAVING count(*)>1 OR {} LIMIT 1''').format(
        identifiers(keys), sql.Identifier('external_source', table), identifiers(keys),
        sql.SQL(' OR ').join(sql.SQL('{} IS NULL').format(sql.Identifier(k)) for k in keys))).fetchone()
    if invalid:
        raise ValueError(f'CSV cursor must be unique and non-null: {table} {keys}')


def read_pg_page(source, schema, table, columns, keys, boundary, limit):
    """Читает ограниченную порцию после позиции и возвращает ключ её последней строки."""
    # В выборке нужны и целевые поля, и ключ источника, даже если ключ не входит в проекцию.
    selected = tuple(dict.fromkeys((*columns, *keys)))
    query = sql.SQL('SELECT {} FROM {}').format(identifiers(selected), sql.Identifier(schema, table))
    params = []
    if boundary is not None:
        # Составные ключи сравниваются целиком в том же порядке, что и при сортировке.
        query += sql.SQL(' WHERE ROW({}) > ROW({})').format(
            identifiers(keys), sql.SQL(',').join(sql.Placeholder() for _ in keys))
        params.extend(boundary)
    query += sql.SQL(' ORDER BY {} LIMIT %s').format(identifiers(keys))
    params.append(limit)
    records = source.execute(query, params).fetchall()
    rows = [tuple(row[selected.index(c)] for c in columns) for row in records]
    last_key = tuple(records[-1][selected.index(k)] for k in keys) if records else None
    return rows, last_key


def load_pg_table(source, conn, batch, schema, table, columns, keys, limit):
    """Доставляет новые строки одной таблицы, фиксируя каждую порцию отдельно."""
    settings_key = f'{schema}.{table}'
    loaded = 0
    while True:
        with conn.transaction():
            state = get_setting(conn, settings_key)
            if state is None and schema == 'external_source':
                # Без ключей в целевой проекции потеря позиции могла бы привести к дублям.
                if conn.execute(sql.SQL('SELECT EXISTS(SELECT 1 FROM {})').format(
                        sql.Identifier('stg', table))).fetchone()[0]:
                    raise ValueError(f'Non-empty STG without checkpoint: {settings_key}')
            boundary = state['last_loaded_key'] if state else None
            rows, last_key = read_pg_page(source, schema, table, columns, keys, boundary, limit)
            if not rows:
                if state is None:
                    save_setting(conn, settings_key, {'last_loaded_key': None})
                break
            write_rows(conn, table, columns, rows, keys if schema == 'source' else ())
            # Строки, триггерный журнал, позиция и счётчик коммитятся вместе при выходе из блока.
            save_setting(conn, settings_key, {'last_loaded_key': checkpoint_key(last_key)})
            record_batch(conn, batch, 'pg_stg', len(rows))
        loaded += len(rows)
        log.info('%s: committed %s rows, cursor=%s', settings_key, len(rows), last_key)
    return loaded


def load_pg(batch):
    """Загружает новые записи шести обычных и трёх внешних таблиц PostgreSQL."""
    limit = batch_limit()
    with connect('SOURCE_PG_DSN') as source, connect() as conn:
        source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        # Снимок фиксирует обычные таблицы; содержимое CSV должно быть стабильно при чтении.
        start_loader(conn, 31001)
        count = 0
        for table, keys in CRAFT_KEYS.items():
            columns = column_names(source, 'source', table)
            with conn.transaction():
                if columns != column_names(conn, 'stg', table):
                    raise ValueError(f'STG/source column mismatch: {table}')
            count += load_pg_table(source, conn, batch, 'source', table, columns, keys, limit)
        for table, columns in EXTERNAL_COLUMNS.items():
            keys = EXTERNAL_KEYS[table]
            check_external_keys(source, table, keys)
            count += load_pg_table(source, conn, batch, 'external_source', table, columns, keys, limit)
        with conn.transaction():
            record_batch(conn, batch, 'pg_stg', 0)
        return count


def mongo_values(document, columns):
    """Преобразует документ в значения столбцов, сохраняя вложенные структуры."""
    unknown = set(document) - set(columns)
    if unknown:
        # Новые атрибуты требуют явного расширения таблицы, иначе данные были бы потеряны.
        raise ValueError(f'Unmapped MongoDB attributes: {sorted(unknown)}')
    result = []
    for column in columns:
        value = document.get(column)
        if value is not None:
            if column == '_id':
                value = str(value)
            elif column in JSON_COLUMNS:
                value = Jsonb(json.loads(json_util.dumps(value)))
            elif column in MONEY_COLUMNS:
                value = Decimal(str(value))
                if not value.is_finite():
                    raise ValueError('Non-finite MongoDB amount')
        result.append(value)
    return tuple(result)


def mongo_boundary(state, upper=False):
    """Строит нижнюю строгую или верхнюю включительную границу по времени и идентификатору."""
    timestamp = datetime.fromisoformat(state['last_update_time'])
    return {'$or': [
        {'update_time': {'$lt' if upper else '$gt': timestamp}},
        {'update_time': timestamp, '_id': {'$lte' if upper else '$gt': state['last_loaded_id']}},
    ]}


def mongo_checkpoint(document):
    """Формирует сохраняемую позицию по последнему документу успешной порции."""
    return {'last_update_time': document['update_time'].isoformat(), 'last_loaded_id': document['_id']}


def load_mongo(batch):
    """Загружает новые и изменённые документы трёх коллекций по полю update_time."""
    limit = batch_limit()
    with MongoClient(os.environ['SOURCE_MONGO_URI'], tz_aware=True, serverSelectionTimeoutMS=10000) as client:
        with connect() as conn:
            start_loader(conn, 31002)
            count = 0
            for table, columns in MONGO_COLUMNS.items():
                collection = mongo_database(client)[table]
                if collection.find_one({'$or': [
                        {'update_time': {'$not': {'$type': 'date'}}},
                        {'_id': {'$not': {'$type': 'string'}}}]}):
                    raise ValueError(f'Mongo cursor requires a BSON date and string _id: {table}')
                last = collection.find_one(sort=[('update_time', -1), ('_id', -1)])
                # Верхняя граница ограничивает текущий проход при появлении новых документов.
                upper = mongo_checkpoint(last) if last else None
                settings_key = f'mongo.{table}'
                while True:
                    with conn.transaction():
                        state = get_setting(conn, settings_key)
                        filters = [mongo_boundary(upper, upper=True)] if upper else []
                        if state and state['last_update_time'] is not None:
                            filters.append(mongo_boundary(state))
                        documents = list(collection.find({'$and': filters}).sort(
                            [('update_time', 1), ('_id', 1)]).limit(limit)) if upper else []
                        if not documents:
                            if state is None:
                                save_setting(conn, settings_key, {'last_update_time': None, 'last_loaded_id': None})
                            break
                        rows = [mongo_values(doc, columns) for doc in documents]
                        # Ошибка любой строки или сохранения позиции откатывает всю текущую порцию.
                        write_rows(conn, table, columns, rows, ('_id',))
                        save_setting(conn, settings_key, mongo_checkpoint(documents[-1]))
                        record_batch(conn, batch, 'mongo_stg', len(rows))
                    count += len(rows)
                    log.info('%s: committed %s rows, cursor=%s', settings_key, len(rows), mongo_checkpoint(documents[-1]))
            with conn.transaction():
                record_batch(conn, batch, 'mongo_stg', 0)
            return count


def validate_stg(batch):
    """Проверяет отметки загрузок, позиции, проекции CSV и наличие триггеров журнала."""
    with connect() as conn:
        counts = {table: conn.execute(sql.SQL('SELECT count(*) FROM {}').format(
            sql.Identifier('stg', table))).fetchone()[0] for table in DATA_TABLES}
        stages = {r[0] for r in conn.execute(
            'SELECT stage FROM etl.loads WHERE batch_id=%s', (batch,))}
        if not {'pg_stg', 'mongo_stg'}.issubset(stages):
            raise AssertionError('Both source tasks must finish before validation')
        expected_settings = {f'source.{t}' for t in CRAFT_KEYS} | {
            f'external_source.{t}' for t in EXTERNAL_COLUMNS} | {f'mongo.{t}' for t in MONGO_COLUMNS}
        actual_settings = {r[0] for r in conn.execute('SELECT settings_key FROM stg.settings')}
        if actual_settings != expected_settings:
            raise AssertionError('Missing or unexpected source checkpoints')
        for table, columns in EXTERNAL_COLUMNS.items():
            if column_names(conn, 'stg', table) != columns:
                raise AssertionError(f'Incorrect external projection: {table}')
        audited = {r[0] for r in conn.execute('''SELECT event_object_table FROM information_schema.triggers
            WHERE event_object_schema='stg' AND trigger_name='audit_changes' ''')}
        if audited != set(DATA_TABLES):
            raise AssertionError('STG audit trigger coverage differs from the 12 data tables')
        conn.execute('''INSERT INTO etl.loads(batch_id,stage,row_count) VALUES (%s,'validate_stg',%s)
            ON CONFLICT(batch_id,stage) DO UPDATE SET row_count=EXCLUDED.row_count,completed_at=now()''',
            (batch, len(DATA_TABLES)))
        return counts
