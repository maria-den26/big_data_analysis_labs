"""Хранение позиций загрузки по образцу examples/stg_settings.py, без классов."""
from psycopg.types.json import Jsonb


def get_setting(conn, settings_key):
    """Возвращает позицию источника или None, если загрузка ещё не начиналась."""
    row = conn.execute(
        'SELECT settings FROM stg.settings WHERE settings_key=%s',
        (settings_key,),
    ).fetchone()
    return row[0] if row else None


def save_setting(conn, settings_key, settings):
    """Создаёт или обновляет позицию, не завершая транзакцию вызывающего загрузчика."""
    # Позиция фиксируется вызывающим кодом вместе со строками соответствующей порции.
    conn.execute('''
        INSERT INTO stg.settings(settings_key,settings) VALUES (%s,%s)
        ON CONFLICT (settings_key) DO UPDATE SET settings=EXCLUDED.settings
        WHERE stg.settings.settings IS DISTINCT FROM EXCLUDED.settings
    ''', (settings_key, Jsonb(settings)))
