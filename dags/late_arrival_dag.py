from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Cấu hình mặc định
default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 13, 0, 0), 
    'end_date': datetime(2026, 3, 26, 23, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# =========================================================================
# DAG 2: LUỒNG BÙ DỮ LIỆU TRỄ CHẠY CUỐI NGÀY (DAILY)
# =========================================================================
with DAG(
    'mdt_late_arrival_reconciliation',
    default_args=default_args,
    description='Job gom dữ liệu trễ và tính toán lại Fact vào cuối ngày',
    schedule_interval='30 0 * * *', # Chạy lúc 00:30 sáng mỗi ngày
    catchup=False, # Không cần catchup quá khứ cho job dọn dẹp
    max_active_runs=1, 
    tags=['mdt', 'late_arrival', 'daily']
) as daily_dag:

    # Task A: Chạy Script 4 để Merge Quarantine vào Silver và xuất file JSON
    task_merge_late_data = SparkSubmitOperator(
        task_id='4_process_late_arrival',
        application='/opt/spark_scripts/4_process_late_arrival.py',
        conn_id='spark_default'
    )

    # Task B: Đọc file JSON và tự động gọi lại Job 3 & 5 cho các ca bị ảnh hưởng
    bash_recalculate_script = """
    if [ -f /tmp/affected_partitions.json ]; then
        echo " Đã tìm thấy danh sách các ca cần chạy lại..."
        
        # Dùng Python để parse JSON và tự động sinh ra các lệnh bash tương ứng
        python3 -c "
import json
try:
    with open('/tmp/affected_partitions.json', 'r') as f:
        data = json.load(f)
        for item in data:
            d = item['date']
            h = item['hour']
            print(f'echo \"🚀 TIẾN HÀNH CHẠY LẠI CA: {d} {h}:00\"')
            print(f'spark-submit /opt/spark_scripts/3_h3_enrichment.py --date {d} --hour {h}')
            print(f'python3 /opt/spark_scripts/5_push_to_postgis.py --date {d} --hour {h}')
except Exception as e:
    print(f'echo \"Lỗi parse JSON: {e}\"')
" > /tmp/run_recalc.sh
        
        # Cấp quyền thực thi và chạy chuỗi lệnh vừa sinh ra
        chmod +x /tmp/run_recalc.sh
        sh /tmp/run_recalc.sh
        
        # Chạy xong thì xóa JSON để reset cho ngày hôm sau
        rm /tmp/affected_partitions.json
    else:
        echo " Không có dữ liệu lệch ca nào. Tuyệt vời!"
    fi
    """

    task_recalculate_facts = BashOperator(
        task_id='recalculate_affected_facts_and_trino',
        bash_command=bash_recalculate_script
    )

    # KẾT NỐI ĐƯỜNG ỐNG DAILY
    task_merge_late_data >> task_recalculate_facts