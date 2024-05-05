from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
import requests
import pandas as pd
import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 1),
}

dag = DAG(
    'openweather_api_dag',
    default_args=default_args,
    description='DAG to run OpenWeather API and fetch data',
    schedule_interval='@once', # timedelta(days=1)
    catchup = False
)

city_name, country_code = ('Ranchi','India')

api_endpoint = 'https://api.openweathermap.org/data/2.5/forecast'
api_params = {
    'q':f'{city_name},{country_code}',
    'appid':{Variable.get('openweather_api_key')}
}

def get_openweather_data(**context):
    try:
        print(f"Starting data extraction from OpenWeather API for {api_params['q']}")
        ti = context['ti']
        response = requests.get(api_endpoint, params=api_params)
        data = response.json()
        df= pd.json_normalize(data['list'])
        # print(df)
        ti.xcom_push(key='final_data', value=df.to_csv(index=False)) # pass on data to the next operator
    except:
        print('An error occured.')
    
# Extract Data From OpenWeather API (JSON) and Convert to CSV
get_data_from_api = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_openweather_data,
    dag=dag,
    provide_context = True
)

# Upload CSV To Already Existing S3 Bucket in AWS
upload_data_to_s3 = S3CreateObjectOperator(
        task_id="upload_data_to_s3",
        aws_conn_id= 'aws_default', # Default AWS connection in Airflow
        s3_bucket='pf-weather-data',
        s3_key='date={{ ds }}/weather_api_data.csv',
        data="{{ ti.xcom_pull(key='final_data') }}",
        dag=dag,
    )

# Trigger DAG To Load Data To Redshift
trigger_transform_redshift_dag = TriggerDagRunOperator(
    task_id="trigger_transform_redshift_dag",
    trigger_dag_id="transform_redshift_dag",  # DAG ID of the appropriate DAG
    dag=dag,
)

get_data_from_api >> upload_data_to_s3 >> trigger_transform_redshift_dag
