from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from polygon import RESTClient
import requests
import json
import pandas as pd

apiKeyStr = '_KFg8zNJe0VmUaYWmKvek8gBErKs22Y1'
client = RESTClient(api_key=apiKeyStr)

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
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        data = json.loads(response.text).data
        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df.timestamp, unit='ms')
        return df
    except Exception as e:
        print(f"Erro ao buscar e parsear dados: {e}")
        return pd.DataFrame()

def calcular_sma(ticker):
    try:
        url = f'https://api.polygon.io/v1/indicators/sma/{ticker}?timespan=minute&adjusted=true&window=50&series_type=close&order=desc&limit=10&apiKey={apiKeyStr}'
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        data = json.loads(response.text)['results']['values']
        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df.timestamp, unit='ms')
        return df
    except Exception as e:
        print(f"Erro ao calcular SMA: {e}")
        return pd.DataFrame()

default_args = {
    'owner': 'Grupo G2',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    'Grupo_G2',
    default_args=default_args,
    description='Um DAG para buscar e processar dados do Polygon',
    schedule_interval='@daily',
    catchup=False
) as dag:

    buscar_agregados_do_polygon_task = PythonOperator(
        task_id='buscar_agregados_do_polygon',
        python_callable=buscar_agregados_do_polygon,
    )

    buscar_e_parsear_task = PythonOperator(
        task_id='buscar_e_parsear',
        python_callable=buscar_e_parsear,
        op_kwargs={'url': f'https://api.polygon.io/v1/indicators/sma/AAPL?ticker=AAPL&timespan=minute&adjusted=true&window=50&series_type=close&order=desc&limit=10&apiKey={apiKeyStr}'},
    )

    calcular_sma_task = PythonOperator(
        task_id='calcular_sma',
        python_callable=calcular_sma,
        op_kwargs={'ticker': 'AAPL'},
    )

buscar_agregados_do_polygon_task >> buscar_e_parsear_task >> calcular_sma_task
