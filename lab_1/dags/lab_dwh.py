"""Расписание и зависимости задач поставки данных из источников в STG."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import dwh_lib as etl


@dag(
    # Обрабатываем текущие поступления раз в десять минут, без запусков за прошлые периоды.
    dag_id='lab_dwh', schedule='*/10 * * * *',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'), catchup=False, max_active_runs=1,
    default_args={'retries': 2, 'retry_delay': timedelta(seconds=20)},
    tags=['lab', 'sources', 'stg'],
    doc_md='Этапы 1–2: инкрементальные порции из трёх источников, позиции в stg.settings и общий журнал stg.logs.',
)
def lab_dwh():
    """Объединяет две независимые загрузки и последующую проверку STG."""
    @task(execution_timeout=timedelta(minutes=5))
    def run_stage(stage):
        """Передаёт идентификатор запуска в выбранную функцию загрузки или проверки."""
        return getattr(etl, stage)(get_current_context()['run_id'])

    pg = run_stage.override(task_id='postgres_to_stg')('load_pg')
    mongo = run_stage.override(task_id='mongo_to_stg')('load_mongo')
    quality = run_stage.override(task_id='validate_stg')('validate_stg')
    #запуск stg→dds
    trigger_dds=TriggerDagRunOperator(
        task_id='trigger_dds',
        trigger_dag_id='lab_dds',
        wait_for_completion=True,#покажет, загрузился ли dds
        poke_interval=10
    )
    # Проверка начинается только после успешного завершения обеих загрузок.
    #stg→dds только после проверки quality
    [pg, mongo] >> quality >> trigger_dds


lab_dwh()
