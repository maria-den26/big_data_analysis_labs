from datetime import timedelta

import dwh_lib as etl
import pendulum
from airflow.decorators import dag, task

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
@dag(
    dag_id = 'lab_dds',
    schedule=None,
    start_date=pendulum.datetime(2024,1,1,tz='UTC'),
    catchup=False,
    max_active_runs=1,
    tags=['lab','dds'],
    doc_md='Этапы 3-4: Формирование схемы звезды: загрузка измерений, загрузка фактов, проверка результата',
)

def lab_dds():
    #загрузка измерений
    @task
    def load_dimensions():
        with etl.connect() as conn:
            etl.execute_file(conn, 'load_dds_dimensions.sql')

    dimensions = load_dimensions()

    #загрузка фактов
    @task
    def load_facts():
        with etl.connect() as conn:
            etl.execute_file(conn,'load_dds_facts.sql')

    facts = load_facts()



    #проверка данных
    @task
    def validate_dds():
        with etl.connect() as conn:
            #простая проверка, что не пусто
            count = conn.execute(
                "SELECT COUNT(*) FROM dds.fact_orders"
            ).fetchone()[0]
            if count==0:
                raise ValueError("Таблица фактов пуста")

            #проверка, на количество заказов
            stg_count=conn.execute(
                "SELECT COUNT(*) FROM stg.craft_market_orders"
            ).fetchone()[0]

            if count!= stg_count:
                raise ValueError(
                    f"Количество заказов не совпадает: STG={stg_count}, DDS={count}"
                )

    trigger_cdm=TriggerDagRunOperator(
        task_id='trigger_cdm',
        trigger_dag_id='lab_cdm',
        wait_for_completion=True,#покажет, загрузился ли dds
        poke_interval=10
    )

    quality=validate_dds()
    #оператор зависимости задача в airflow
    dimensions >> facts >> quality>>trigger_cdm
lab_dds()