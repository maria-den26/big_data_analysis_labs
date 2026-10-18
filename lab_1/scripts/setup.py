"""Создание схем лабораторной и первоначальное наполнение источников."""
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient
from dwh_lib import connect, execute_file, mongo_database


def normalize(value):
    """Рекурсивно преобразует идентификаторы и даты из исходного формата JSON."""
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if not isinstance(value, dict):
        return value
    if set(value) == {'$oid'}:
        # Выданный идентификатор ресторана не соответствует ObjectId; сохраняем исходный текст.
        return value['$oid']
    if set(value) == {'$date'}:
        return datetime.fromtimestamp(int(value['$date']['$numberLong']) / 1000, tz=timezone.utc)
    return {key: normalize(v) for key, v in value.items()}


def initialize_databases():
    """Создаёт структуру и индексы, наполняя источники только при первом запуске."""
    with connect('SOURCE_PG_DSN') as conn:
        execute_file(conn, 'craft_source.sql')
        execute_file(conn, 'external_source.sql')
        conn.execute('''CREATE TABLE IF NOT EXISTS public.lab_initialized (
            initialized_at timestamptz NOT NULL DEFAULT now(),
            mongo_initialized boolean NOT NULL DEFAULT false)''')
        if not conn.execute('SELECT 1 FROM public.lab_initialized').fetchone():
            # Отметка и учебные записи сохраняются в одной транзакции PostgreSQL.
            execute_file(conn, 'demo_craft.sql')
            conn.execute('INSERT INTO public.lab_initialized DEFAULT VALUES')
    with connect() as conn:
        # Схемы детального слоя и витрин пока пустые; их объекты будут созданы на следующих этапах.
        conn.execute('CREATE SCHEMA IF NOT EXISTS dds')
        conn.execute('CREATE SCHEMA IF NOT EXISTS cdm')
        execute_file(conn, 'stg_source.sql')
        execute_file(conn, 'stg.sql')
    with MongoClient(os.environ['SOURCE_MONGO_URI'], serverSelectionTimeoutMS=10000) as client:
        db = mongo_database(client)
        with connect('SOURCE_PG_DSN') as conn:
            seeded = conn.execute('SELECT mongo_initialized FROM public.lab_initialized').fetchone()[0]
        if not seeded:
            # Повтор после сбоя не заменяет документы: записываются только отсутствующие идентификаторы.
            for filename, collection in [('clients', 'clients'), ('restaurants', 'restaurant'), ('orders', 'orders')]:
                raw = json.loads((Path('/opt/airflow/source-data/json') / f'{filename}.json').read_text(encoding='utf-8-sig'))
                for doc in normalize(raw if isinstance(raw, list) else [raw]):
                    db[collection].update_one({'_id': doc['_id']}, {'$setOnInsert': doc}, upsert=True)
            with connect('SOURCE_PG_DSN') as conn:
                conn.execute('UPDATE public.lab_initialized SET mongo_initialized=true')
        for collection in ('clients', 'restaurant', 'orders'):
            # Индекс соответствует сортировке и границе порций инкрементального загрузчика.
            db[collection].create_index([('update_time', 1), ('_id', 1)])


def initialize_airflow_user():
    """Создаёт локального администратора Airflow, если такой пользователь отсутствует."""
    users = subprocess.run(['airflow', 'users', 'list', '--output', 'json'], check=True,
                           capture_output=True, text=True).stdout
    if '"username": "admin"' not in users and '"username":"admin"' not in users:
        subprocess.run(['airflow', 'users', 'create', '--username', 'admin', '--password', 'admin',
                        '--firstname', 'Lab', '--lastname', 'Admin', '--role', 'Admin',
                        '--email', 'admin@example.test'], check=True)


if __name__ == '__main__':
    initialize_databases()
    initialize_airflow_user()
    print('Sources, STG and Airflow user initialized.')
