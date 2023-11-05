from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from typing import Any, Dict
from utils.dag import DAG_DEFAULT_ARGS
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowFailException
import requests
import json
import time
import psycopg2
from psycopg2 import extras

dag_default_args: Dict[str, Any] = {
    **DAG_DEFAULT_ARGS,
    'start_date': datetime(2023,7,7),
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

def task_fail(context):
    # 실패 메시지를 출력하고 Airflow 예외를 발생시킴으로써 DAG를 종료.
    task_instance = context['task_instance']
    print(f"Task {task_instance.task_id} 실패!")
    raise AirflowFailException("Task 실패로 인해 전체 DAG 종료.")

def get_day_candle_bulk(task_date):
    #url = "https://api.upbit.com/v1/candles/days?market=KRW-BTC&to="+"2020-01-01 00:00:00"+"&count=1"
    formatted_execution_date = task_date.strftime("%Y-%m-%d %H:%M:%S")
    #print(formatted_execution_date)
    url = f"https://api.upbit.com/v1/candles/days?market=KRW-BTC&to={formatted_execution_date}&count=200"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
                
    return response.text

with DAG(
    dag_id='day_candle_default_bulk',
    default_args=dag_default_args,
    description='get KRW-BTC day candle first data bulk',
    schedule_interval=None
) as dag:
    
    def save_to_bulklist(**context):
        base_date = datetime.now()
        bulk_list = []

        for i in range(11):
            task_date = base_date - timedelta(days=200 * i)
            candle_list = get_day_candle_bulk(task_date)
            candle_json = json.loads(candle_list)
            #print(i)
            #print(candle_list)
            #print(candle_json)
            bulk_list.extend(candle_json)
            time.sleep(1)
            
        bulk_tuple = [tuple(item.values()) for item in bulk_list]
        
        context['task_instance'].xcom_push(key='bulk_tuple', value=bulk_tuple)
    
    get_bulk_data = PythonOperator(
        task_id='get_bulk_data',
        python_callable=save_to_bulklist,
        provide_context=True,  # 이를 설정하면, callable 함수에 context를 전달
        on_failure_callback=task_fail,
        dag=dag
    )
       
    def bulk_insert(**context):
        data = context['task_instance'].xcom_pull(task_ids='get_bulk_data', key='bulk_tuple')
        insert_query = """
            INSERT INTO day_candle (market, candle_date_time_utc, candle_date_time_kst, opening_price, high_price, low_price, trade_price, last_timestamp, candle_acc_trade_price, candle_acc_trade_volume, prev_closing_price, change_price, change_rate)
            VALUES %s
        """
        print(data)
        conn = psycopg2.connect("dbname=upbit user=airflow password=airflow host=postgres_analytics port=5432")
        cur = conn.cursor()
        #cur.execute(insert_query, data)
        extras.execute_values(cur, insert_query, data)
        conn.commit()
        cur.close()
        conn.close()
            
    insert_local = PythonOperator(
        task_id='insert_local',
        python_callable=bulk_insert,
        provide_context=True,
        on_failure_callback=task_fail,
        dag=dag
    )
    get_bulk_data >> insert_local