from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from typing import Any, Dict
from utils.dag import DAG_DEFAULT_ARGS
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import json

dag_default_args: Dict[str, Any] = {
    **DAG_DEFAULT_ARGS,
    'start_date': datetime(2020, 1, 1),
    'max_active_tis_per_dag': 2,
}

# dag_default_args: Dict[str, Any] = {
#     'weight_rule': 'absolute',
#     'owner': 'myupbit',
#     'depends_on_past': False,
#     'retry_delay': timedelta(minutes=5),
#     'retries': 1,
#     'start_date': datetime(2023, 1, 1),
#     'max_active_tis_per_dag': 2,
# }

with DAG(
    dag_id='day_candle_api',
    default_args=dag_default_args,
    description='get KRW-BTC day candle',
    schedule_interval='@once'
) as dag:
    
    def get_day_candle(**context):
        #url = "https://api.upbit.com/v1/candles/days?market=KRW-BTC&to="+"2020-01-01 00:00:00"+"&count=1"
        execution_date = context['execution_date']
        print(execution_date)
        formatted_execution_date = execution_date.strftime("%Y-%m-%d %H:%M:%S")
        url = f"https://api.upbit.com/v1/candles/days?market=KRW-BTC&to={formatted_execution_date}&count=1"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        return response.text
    
    t1 = PythonOperator(
        task_id='day_candle_api',
        python_callable=get_day_candle,
        dag=dag
    )
    
    def save_to_postgres(**context):
        response_text = context['task_instance'].xcom_pull(task_ids='day_candle_api')
        #print(response_text)
        data = json.loads(response_text)
        
        context['task_instance'].xcom_push(key='market', value=data[0]['market'])
        context['task_instance'].xcom_push(key='candle_date_time_utc', value=data[0]['candle_date_time_utc'])
        context['task_instance'].xcom_push(key='candle_date_time_kst', value=data[0]['candle_date_time_kst'])
        context['task_instance'].xcom_push(key='opening_price', value=data[0]['opening_price'])
        context['task_instance'].xcom_push(key='high_price', value=data[0]['high_price'])
        context['task_instance'].xcom_push(key='low_price', value=data[0]['low_price'])
        context['task_instance'].xcom_push(key='trade_price', value=data[0]['trade_price'])
        context['task_instance'].xcom_push(key='timestamp', value=data[0]['timestamp'])
        context['task_instance'].xcom_push(key='candle_acc_trade_price', value=data[0]['candle_acc_trade_price'])
        context['task_instance'].xcom_push(key='candle_acc_trade_volume', value=data[0]['candle_acc_trade_volume'])
        context['task_instance'].xcom_push(key='prev_closing_price', value=data[0]['prev_closing_price'])
        context['task_instance'].xcom_push(key='change_price', value=data[0]['change_price'])
        context['task_instance'].xcom_push(key='change_rate', value=data[0]['change_rate'])
    
    t2 = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
        provide_context=True,  # 이를 설정하면, callable 함수에 context를 전달
        dag=dag
    )
    
    insert_local = PostgresOperator(
        task_id='insert_local',
        sql="""
            INSERT INTO day_candle (market,candle_date_time_utc,candle_date_time_kst,opening_price,high_price,low_price,trade_price,last_timestamp,candle_acc_trade_price,candle_acc_trade_volume,prev_closing_price,change_price,change_rate)
            VALUES (
                '{{ ti.xcom_pull(task_ids='save_to_postgres', key='market') }}',
                '{{ ti.xcom_pull(task_ids='save_to_postgres', key='candle_date_time_utc') }}',
                '{{ ti.xcom_pull(task_ids='save_to_postgres', key='candle_date_time_kst') }}',
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='opening_price') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='high_price') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='low_price') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='trade_price') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='timestamp') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='candle_acc_trade_price') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='candle_acc_trade_volume') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='prev_closing_price') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='change_price') }},
                {{ ti.xcom_pull(task_ids='save_to_postgres', key='change_rate') }}
            )
        """,
        postgres_conn_id="docker_postgres"
    )
    
    t1 >> t2 >> insert_local