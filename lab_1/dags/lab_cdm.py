"""Загрузка аналитической витрины CDM."""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

import dwh_lib as etl


@dag(
    dag_id="lab_cdm",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=20),
    },
    tags=["lab", "cdm"],
)
def lab_cdm():

    @task
    def load_customer_report():
        """Пересчитывает месячную витрину покупателей."""

        with etl.connect() as conn:
            etl.execute_file(conn, "load_cdm_customer_report.sql")

    @task
    def validate_cdm():
        """Проверяет заполнение витрины."""

        with etl.connect() as conn:
            result = conn.execute(
                """SELECT COUNT(*) FROM cdm.customer_report """).fetchone()[0]

            if result == 0:
                raise ValueError("CDM customer_report is empty")

            return result

    load_customer_report() >> validate_cdm()


lab_cdm()


