from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 4, tzinfo=local_tz), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mdt_daily_prophet_forecast',
    default_args=default_args,
    description='Pipeline chạy dự báo đa biến Prophet & Đánh giá MAPE (Tối ưu RAM & Arrow)',
    schedule_interval=None, 
    catchup=False,          
    max_active_runs=1, 
    tags=['mdt', 'prophet', 'forecast', 'daily']
) as dag:

    # Tham số ngày chạy
    current_vn_date = "2025-05-13"
    yesterday_vn_date = "2025-05-12"

    # =======================================================
    # BẬT ARROW VÀ ÉP PARTITIONS ĐỂ TRÁNH QUÁ TẢI RAM HOST
    # =======================================================
    forecast_args = [
        '--date', current_vn_date, 
        '--hour', '01',
        '--lookback_days', '7'
    ]

    # 2. Đưa các cấu hình của Spark vào một dictionary
    spark_configs = {
        'spark.sql.execution.arrow.pyspark.enabled': 'true',
        'spark.sql.shuffle.partitions': '4'
    }

    # 3. Truyền riêng rẽ vào Operator
    task_run_forecast = SparkSubmitOperator(
        task_id='1_run_prophet_multivariate',
        application='/opt/spark_scripts/6_forecast.py',
        conn_id='spark_default',
        executor_memory='1536m',
        driver_memory='1g',
        execution_timeout=timedelta(minutes=90),
        application_args=forecast_args,  # <--- Python nhận cái này
        conf=spark_configs               # <--- Spark nhận cái này
    )

    evaluate_args = [
        '--eval_date', yesterday_vn_date 
    ]

    task_evaluate_mape = SparkSubmitOperator(
        task_id='2_evaluate_yesterday_mape',
        application='/opt/spark_scripts/6b_evaluate.py', 
        conn_id='spark_default',
        execution_timeout=timedelta(minutes=15),
        application_args=evaluate_args
    )
    
    task_register_trino = BashOperator(
        task_id='3_publish_forecast_to_trino',
        bash_command=f'python /opt/spark_scripts/8_register_forecast.py --date {current_vn_date} --hour 01'
    )
    
    task_run_forecast >> task_evaluate_mape >> task_register_trino