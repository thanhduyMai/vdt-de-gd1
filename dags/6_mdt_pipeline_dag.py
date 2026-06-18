from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import time
import stat
import paramiko

def fetch_sftp_to_minio_landing(execution_date, **kwargs):
    # 1. Trích xuất thời gian chạy (YYYYMMDD và HH)
    date_folder = execution_date.strftime('%Y%m%d') # VD: 20250504
    hour = execution_date.strftime('%H')            # VD: 00
    file_name = f"mdt_{date_folder}{hour}.csv"      # VD: mdt_2025050400.csv
    
    # 2. Cấu hình kết nối
    conn_id = 'mdt_sftp_server' # Tên connection bạn đặt trên Airflow UI
    print(f" Đang đọc cấu hình '{conn_id}' từ Airflow...")
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as e:
        raise ValueError(f"Không tìm thấy Connection '{conn_id}': {e}")

    host = conn.host
    port = conn.port if conn.port else 22
    user = conn.login
    password = conn.password

    # 3. Đường dẫn
    remote_file = f"/u01/vdt-data-de/mdt/{date_folder}/{file_name}"
    local_file = f"/tmp/{file_name}"
    minio_bucket = Variable.get("minio_landing_bucket", default_var="landingzone")

    print(f" Bắt đầu kéo dữ liệu từ {user}@{host}:{port}...")
    print(f"  -> File Nguồn: {remote_file}")
    
    # ==========================================
    # PHẦN 1: TẢI TỪ SFTP VỀ WORKER (Có tính giờ)
    # ==========================================
    download_time = 0.0
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # Kiểm tra file có tồn tại trên server không
        try:
            sftp.stat(remote_file)
        except IOError:
            raise FileNotFoundError(f"File không tồn tại trên SFTP server: {remote_file}")

        print(f" Đang tải {file_name}... ", end='', flush=True)
        start_time = time.time()
        
        # Lấy file về /tmp/ của Airflow Worker
        sftp.get(remote_file, local_file)
        
        download_time = time.time() - start_time
        print(f" ({download_time:.3f}s)")

        sftp.close()
        transport.close()
    except Exception as e:
        raise RuntimeError(f" LỖI TRONG QUÁ TRÌNH TẢI SFTP: {e}")

    # ==========================================
    # PHẦN 2: ĐẨY TỪ WORKER LÊN MINIO LAKESOUSE
    # ==========================================
    print(f" Đang upload file lên MinIO: s3a://{minio_bucket}/{file_name}...")
    s3_start_time = time.time()
    
    s3_hook = S3Hook(aws_conn_id='minio_default')
    s3_hook.load_file(
        filename=local_file,
        key=file_name,
        bucket_name=minio_bucket,
        replace=True
    )
    
    upload_time = time.time() - s3_start_time
    
    # Xóa file tạm tại worker để tránh đầy ổ cứng
    if os.path.exists(local_file):
        os.remove(local_file)

    # ==========================================
    # BÁO CÁO HIỆU NĂNG
    # ==========================================
    print('\n=======================================================')
    print(f' BÁO CÁO HIỆU NĂNG TẢI DỮ LIỆU CA {date_folder}-{hour}')
    print('=======================================================')
    print(f' Thời gian tải (SFTP -> Worker)  : {download_time:.3f} giây')
    print(f' Thời gian lưu (Worker -> MinIO) : {upload_time:.3f} giây')
    print(f' Tổng thời gian Ingestion        : {download_time + upload_time:.3f} giây')
    print(f' Dữ liệu đích sẵn sàng tại       : s3a://{minio_bucket}/{file_name}')
    print('=======================================================\n')

    return f"s3a://{minio_bucket}/{file_name}"

# 1. Cấu hình mặc định DAG
default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 4, 0, 0), 
    'end_date': datetime(2025, 5, 31, 23, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Khởi tạo DAG
with DAG(
    'mdt_spatial_analytics_pipeline',
    default_args=default_args,
    description='Pipeline MDT Lakehouse Star Schema với SFTP Paramiko',
    schedule_interval='@hourly', 
    catchup=True, 
    max_active_runs=1, 
    tags=['mdt', 'star_schema', 'sftp']
) as dag:

    common_args = [
        '--date', '{{ ds }}', 
        '--hour', '{{ execution_date.strftime("%H") }}'
    ]
    
    # Tham số động truyền đường dẫn MinIO vừa tải về sang cho Spark
    ingest_args = [
        '--type', 'mdt',
        '--date', '{{ ds }}',
        '--hour', '{{ execution_date.strftime("%H") }}',
        '--input_path', "s3a://" + Variable.get("minio_landing_bucket", "landingzone") + "/mdt_{{ execution_date.strftime('%Y%m%d%H') }}.csv"
    ]

    # TASK 0: CHẠY HÀM PYTHON Ở TRÊN (Lấy file SFTP -> MinIO)
    task_fetch_sftp = PythonOperator(
        task_id='0_fetch_sftp_to_minio',
        python_callable=fetch_sftp_to_minio_landing,
    )

    task_ingest = SparkSubmitOperator(
        task_id='1_ingest_to_bronze',
        application='/opt/spark_scripts/1_ingest_data.py', 
        conn_id='spark_default', 
        application_args=ingest_args 
    )

    task_quality = SparkSubmitOperator(
        task_id='2_run_quality_check',
        application='/opt/spark_scripts/2_quality_check.py',
        conn_id='spark_default',
        execution_timeout=timedelta(minutes=10),
        application_args=common_args
    )

    task_fact_builder = SparkSubmitOperator(
        task_id='3_build_fact_model',
        application='/opt/spark_scripts/3_h3_enrichment.py', 
        conn_id='spark_default',
        application_args=common_args
    )

    task_forecast = SparkSubmitOperator(
        task_id='6_forecast',
        application='/opt/spark_scripts/6_forecast.py',
        conn_id='spark_default',
        execution_timeout=timedelta(minutes=30), 
        application_args=common_args
    )

    task_publish_trino = BashOperator(
        task_id='5_publish_to_trino',
        bash_command='python /opt/spark_scripts/5_push_to_postgis.py --date {{ ds }} --hour {{ execution_date.strftime("%H") }}'
    )

    # 3. KẾT NỐI ĐƯỜNG ỐNG
    task_fetch_sftp >> task_ingest >> task_quality >> task_fact_builder >> task_forecast >> task_publish_trino
