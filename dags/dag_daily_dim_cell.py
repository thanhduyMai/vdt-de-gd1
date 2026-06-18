from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import time
import paramiko

def fetch_cell_sftp_to_landing(**kwargs):
    conn_id = 'mdt_sftp_server'
    conn = BaseHook.get_connection(conn_id)

    host = conn.host
    port = conn.port if conn.port else 22
    user = conn.login
    password = conn.password

    # Đường dẫn chuẩn theo yêu cầu của bác
    remote_file = "/u01/vdt-data-de/cell-info/cell_info.csv"
    local_file = "/tmp/cell_info.csv"
    minio_bucket = Variable.get("minio_landing_bucket", default_var="landingzone")

    print(f" Bắt đầu kéo file Dim Cell từ SFTP: {remote_file}")
    
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        sftp.get(remote_file, local_file)
        
        sftp.close()
        transport.close()
    except Exception as e:
        raise RuntimeError(f" Lỗi tải SFTP Cell Info: {e}")

    print(f" Đang upload lên s3a://{minio_bucket}/cell_info.csv...")
    s3_hook = S3Hook(aws_conn_id='minio_default')
    s3_hook.load_file(
        filename=local_file,
        key="cell_info.csv",
        bucket_name=minio_bucket,
        replace=True
    )
    
    if os.path.exists(local_file):
        os.remove(local_file)

    return f"s3a://{minio_bucket}/cell_info.csv"

# Cấu hình DAG
default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 4), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_dim_cell_update',
    default_args=default_args,
    description='Kéo và cập nhật (SCD1) bảng Dim Cell hằng ngày',
    schedule_interval='30 23 * * *', # Chạy lúc 23:30 hằng ngày
    catchup=False, 
    max_active_runs=1,
    tags=['cell', 'scd1', 'daily']
) as dag:

    # TASK 1: Kéo file từ SFTP
    task_fetch_cell = PythonOperator(
        task_id='fetch_cell_info',
        python_callable=fetch_cell_sftp_to_landing,
    )

    # TASK 2: Chạy PySpark Upsert
    task_update_cell = SparkSubmitOperator(
        task_id='update_dim_cell_delta',
        application='/opt/spark_scripts/0_ingest_cell.py', 
        conn_id='spark_default',
    )

    task_fetch_cell >> task_update_cell