# Общий образ для инициализации, планировщика и веб-интерфейса Airflow.
FROM apache/airflow:2.11.0-python3.12
# Версии закреплены для воспроизводимой установки клиентов PostgreSQL и MongoDB.
RUN pip install --no-cache-dir "apache-airflow==2.11.0" "pymongo==4.11.3" "psycopg[binary]==3.2.6"
# Исходники включены в образ, поэтому пересборка учитывает изменения проекта.
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root sql/ /opt/airflow/sql/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
COPY --chown=airflow:root tests/ /opt/airflow/tests/
