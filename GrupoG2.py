from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from polygon import RESTClient
import requests
import json
import pandas as pd

apiKeyStr = '_KFg8zNJe0VmUaYWmKvek8gBErKs22Y1'
client = RESTClient(api_key=apiKeyStr)

args_padrao = {
    'owner': 'Grupo G2',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'Grupo_G2',
    default_args=args_padrao,
    description='Um DAG para buscar e processar dados do Polygon',
    schedule_interval='@daily',
)

def buscar_agregados_do_polygon():
    aggs = client.get_aggs(
        ticker="AAPL",
        multiplier=1,
        timespan="day",
        from_="2022-04-04",
        to="2023-04-04"
    )
    aggs_df = pd.DataFrame(aggs)
    aggs_df['data'] = pd.to_datetime(aggs_df.timestamp, unit='ms')
    return aggs_df

def buscar_e_parsear(url):
    response = requests.get(url)
    data = json.loads(response.text).data
    df = pd.DataFrame(data)
    df['data'] = pd.to_datetime(df.timestamp,unit='ms')
    return df

def calcular_sma(ticker):
    url = 'https://api.polygon.io/v1/indicators/sma/AAPL?timespan=minute&adjusted=true&window=50&series_type=close&order=desc&limit=10&apiKey=_KFg8zNJe0VmUaYWmKvek8gBErKs22Y1'
    response = requests.get(url)
    data = json.loads(response.text)['results']['values']
    df = pd.DataFrame(data)
    df['data'] = pd.to_datetime(df.timestamp,unit='ms')
    return df

buscar_agregados_do_polygon_task = PythonOperator(
    task_id='buscar_agregados_do_polygon',
    python_callable=buscar_agregados_do_polygon,
    dag=dag,
)

buscar_e_parsear_task = PythonOperator(
    task_id='buscar_e_parsear',
    python_callable=buscar_e_parsear,
    op_kwargs={'url': 'https://api.polygon.io/v1/indicators/sma/AAPL?ticker=AAPL&timespan=minute&adjusted=true&window=50&series_type=close&order=desc&limit=10&apiKey=_KFg8zNJe0VmUaYWmKvek8gBErKs22Y1'},
    dag=dag,
)

calcular_sma_task = PythonOperator(
    task_id='calcular_sma',
    python_callable=calcular_sma,
    op_kwargs={'ticker': 'AAPL'},
    dag=dag,
)

buscar_agregados_do_polygon_task >> buscar_e_parsear_task >> calcular_sma_task
