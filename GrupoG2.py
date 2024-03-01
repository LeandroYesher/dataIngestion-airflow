from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from polygon import RESTClient
import pandas as pd

polygonApiKey = Variable.get("POLYGON_API_KEY")
client = RESTClient(api_key=polygonApiKey)

def buscar_agregados_do_polygon(ticker):
    data_ontem = datetime.now() - timedelta(days=1)
    data_ontem_formatada = data_ontem.strftime("%Y-%m-%d")

    dados = client.get_aggs(
        ticker=ticker,
        multiplier=1,
        timespan="day",
        from_="2023-04-04",
        to=data_ontem_formatada
    )
    df = pd.DataFrame(dados)
    df['data'] = pd.to_datetime(df.timestamp, unit='ms')
    return df

def salvar_dataframe(task_instance):

    df = task_instance.xcom_pull(task_ids='buscar_agregados_do_polygon')
    projeto_id = Variable.get("PROJETO_ID")
    dataset_id = Variable.get("DATASET_ID")
    nome_tabela = Variable.get("NOME_TABELA")

    try:
        df.to_gbq(
            f"{projeto_id}.{dataset_id}.{nome_tabela}", 
            project_id = projeto_id, 
            if_exists="replace",
            auth_local_webserver=False
        )
        print("DataFrame enviado com sucesso")
    except Exception as e:
        print(f"Erro ao enviar o DataFrame: {str(e)}")

args_padrao = {
    'owner': 'Grupo G2',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    'GrupoG2',
    default_args=args_padrao,
    description='Um DAG para buscar e processar dados do Polygon',
    schedule_interval='@daily',
    catchup=False) as dag:

    buscar_agregados_do_polygon_task = PythonOperator(
        task_id='buscar_agregados_do_polygon',
        python_callable=buscar_agregados_do_polygon,
        op_kwargs={"ticker": "AAPL"},
        dag=dag,
    )

    salvar_dataframe_task = PythonOperator(
        task_id="salvar_dataframe",
        python_callable=salvar_dataframe,
        dag=dag,
    )

buscar_agregados_do_polygon_task >> salvar_dataframe_task
