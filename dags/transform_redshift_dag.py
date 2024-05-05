from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta

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
    'transform_redshift_dag',
    default_args=default_args,
    description='DAG to run OpenWeather API and fetch data', 
    schedule_interval=timedelta(days=1), # '@once'
    catchup=False
)

# Defining the Glue Job
transform_task = GlueJobOperator(
    task_id='weather_transform_task',
    job_name='weather_transform_job',
    script_location='s3://aws-glue-assets-654654491149-us-east-1/scripts/weather_data_ingestion.py',
    s3_bucket='s3://aws-glue-assets-654654491149-us-east-1',  # S3 bucket where logs and local etl script will be uploaded
    aws_conn_id='aws_default',  # Default AWS connection in Airflow
    region_name="us-east-1",
    iam_role_name='glue-role',
    create_job_kwargs ={"GlueVersion": "4.0", "NumberOfWorkers": 2, "WorkerType": "G.1X", "Connections":{"Connections":["redshift-connection"]},},
    dag=dag,
)

# Define Task Dependencies
# transform_task