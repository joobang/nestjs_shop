"""
비트코인의 중장기 트렌드를 파악하기 위한 테이블. 
이동 평균, 지수 이동 평균 등의 기법을 활용해 트렌드를 측정할 수 있다.
CREATE TABLE trend AS
SELECT candle_date_time_utc,
	   high_price - low_price as daily_volatility,
       AVG(trade_price) OVER (ORDER BY candle_date_time_utc ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as moving_avg_month,
       AVG(trade_price) OVER (ORDER BY candle_date_time_utc ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_week
FROM day_candle;
"""
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

with DAG(
    dag_id='candle_mart_batch',
    default_args=dag_default_args,
    description='get KRW-BTC candle mart',
    schedule_interval='@once'
) as dag:
    
    insert_local = PostgresOperator(
        task_id='create_mart_batch',
        sql="""
            DROP TABLE IF EXISTS candle_trend;
            CREATE TABLE candle_trend AS
            SELECT candle_date_time_utc,
	            high_price - low_price as daily_volatility,
                AVG(trade_price) OVER (ORDER BY candle_date_time_utc ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as moving_avg_month,
                AVG(trade_price) OVER (ORDER BY candle_date_time_utc ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_week
            FROM day_candle;
        """,
        postgres_conn_id="docker_postgres"
    )
    
    insert_local