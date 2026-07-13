from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook 
from datetime import datetime, timedelta

def check_and_send_alert(**kwargs):
    import trino
    import pandas as pd
    from datetime import datetime
    
    target_date = kwargs['dag_run'].conf.get('target_date', '2025-05-11')
    critical_hours = kwargs['dag_run'].conf.get('critical_hours', 3)
    
    print(f"==================================================")
    print(f"[MDT ALERT SYSTEM] Đang xử lý ngày: {target_date}")
    print(f"==================================================")

    conn = None
    try:
        conn = trino.dbapi.connect(
            host="mdt_trino", port=8080, user="admin", catalog="lakehouse", schema="default"
        )
        cur = conn.cursor()
        
        cur.execute(f"SELECT MAX(CAST(hour AS INTEGER)) FROM lakehouse.default.fact_mdt_hourly WHERE date = '{target_date}'")
        max_hour = cur.fetchone()[0]
        if max_hour is None: max_hour = 0
        
        ref_time_str = f"{target_date} {max_hour:02d}:00:00"
        
        # Sửa logic Forecast: Quét toàn bộ rủi ro trong 12 tiếng tiếp theo thay vì chỉ 1 tiếng
        query = f"""
            WITH BaseData AS (
                SELECT cell_id, avg_rsrp, user_density_count, CAST(hour AS INTEGER) AS hour_num
                FROM lakehouse.default.fact_mdt_hourly
                WHERE date = '{target_date}'
            ),
            CurrentSeverity AS (
                SELECT cell_id, avg_rsrp, user_density_count, hour_num,
                       SUM(CASE WHEN avg_rsrp < -100 THEN 1 ELSE 0 END) OVER (PARTITION BY cell_id ORDER BY hour_num ASC ROWS BETWEEN {critical_hours - 1} PRECEDING AND CURRENT ROW) AS bad_hours
                FROM BaseData
            ),
            AlertCurrent AS (
                SELECT cell_id, avg_rsrp, user_density_count, 
                       CASE WHEN bad_hours >= {critical_hours} THEN 'Nghiêm trọng (Lõm sóng liên tục)' ELSE 'Lõm sóng hiện tại' END AS alert_level
                FROM CurrentSeverity
                WHERE hour_num = {max_hour} AND avg_rsrp < -100 AND user_density_count >= 50
            ),
            AlertForecast AS (
                SELECT 
                    cell_id, 
                    forecast_rsrp AS avg_rsrp, 
                    CAST(forecast_density AS INTEGER) AS user_density_count, 
                    'Dự báo (' || LPAD(CAST(EXTRACT(HOUR FROM (CAST(forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR)) AS VARCHAR), 2, '0') || ':00): ' || 
                    CASE 
                        WHEN (forecast_rsrp - forecast_rsrp_lower) > 10 AND forecast_density > (forecast_upper * 0.5) THEN 'Nguy cơ suy giảm đột ngột'
                        WHEN forecast_rsrp < -100 THEN 'Tiếp tục lõm sóng'
                    END AS alert_level
                FROM lakehouse.default.fact_forecast_hourly
                WHERE date_trunc('hour', CAST(forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) > date_parse('{ref_time_str}', '%Y-%m-%d %H:%i:%s')
                  AND date_trunc('hour', CAST(forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) <= date_add('hour', 12, date_parse('{ref_time_str}', '%Y-%m-%d %H:%i:%s'))
                  AND (
                      ((forecast_rsrp - forecast_rsrp_lower) > 10 AND forecast_density > (forecast_upper * 0.5))
                      OR (forecast_rsrp < -100)
                  )
            )
            SELECT * FROM AlertCurrent
            UNION ALL
            SELECT * FROM AlertForecast
        """
        
        df_alert = pd.read_sql_query(query, conn)
        
        # Phân tách bảng
        df_current = df_alert[df_alert['alert_level'].str.contains('Nghiêm trọng|hiện tại', case=False, na=False)]
        df_forecast = df_alert[df_alert['alert_level'].str.contains('Dự báo', case=False, na=False)]
        
        print(f"Cảnh báo hiện tại: {len(df_current)} | Cảnh báo dự báo: {len(df_forecast)}")

        if len(df_alert) > 0:
            html_content = f"""
            <div style="font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; max-width: 650px;">
                <h2 style="color: #d32f2f; border-bottom: 2px solid #d32f2f; padding-bottom: 10px;">CẢNH BÁO TỪ HỆ THỐNG G-SQM</h2>
                <p>Hệ thống ghi nhận sự kiện/rủi ro suy giảm chất lượng phủ sóng tại khung giờ <b>{max_hour}:00</b> ngày <b>{target_date}</b>.</p>
            """
            
            # --- BẢNG HIỆN TẠI ---
            table_rows = "".join([
                f"<tr style='background-color: {'#ffebee' if 'Nghiêm trọng' in r['alert_level'] else '#ffffff'};'>"
                f"<td style='padding:8px; border:1px solid #ddd;'><b>{r['cell_id']}</b></td>"
                f"<td style='padding:8px; border:1px solid #ddd; color: #d32f2f;'>{r['avg_rsrp']:.2f}</td>"
                f"<td style='padding:8px; border:1px solid #ddd;'>{r['user_density_count']}</td>"
                f"<td style='padding:8px; border:1px solid #ddd;'>{r['alert_level']}</td></tr>" 
                for _, r in df_current.iterrows()
            ]) if not df_current.empty else "<tr><td colspan='4' style='padding:10px; text-align:center;'>Không có cảnh báo hiện tại.</td></tr>"
            
            html_content += f"""
            <h3 style="color: #424242;">1. Tình trạng Hiện tại</h3>
            <table style="border-collapse: collapse; width: 100%; text-align: left; margin-bottom: 20px;">
                <tr style="background-color: #f2f2f2;">
                    <th style="padding:10px; border:1px solid #ddd;">Cell ID</th>
                    <th style="padding:10px; border:1px solid #ddd;">RSRP (dBm)</th>
                    <th style="padding:10px; border:1px solid #ddd;">Thiết bị</th>
                    <th style="padding:10px; border:1px solid #ddd;">Mức độ</th>
                </tr>
                {table_rows}
            </table>
            """

            # --- BẢNG DỰ BÁO (QUÉT 12H) ---
            table_rows_fc = "".join([
                f"<tr><td style='padding:8px; border:1px solid #ddd;'><b>{r['cell_id']}</b></td>"
                f"<td style='padding:8px; border:1px solid #ddd; color: #f57c00;'>{r['avg_rsrp']:.2f}</td>"
                f"<td style='padding:8px; border:1px solid #ddd;'>{r['user_density_count']}</td>"
                f"<td style='padding:8px; border:1px solid #ddd;'>{r['alert_level']}</td></tr>" 
                for _, r in df_forecast.iterrows()
            ]) if not df_forecast.empty else "<tr><td colspan='4' style='padding:10px; text-align:center;'>Trạng thái: An toàn. Không ghi nhận nguy cơ lõm sóng trong các giờ tới.</td></tr>"
            
            html_content += f"""
            <h3 style="color: #f57c00;">2. Dự báo Rủi ro Không gian (Trong 12 giờ tới)</h3>
            <table style="border-collapse: collapse; width: 100%; text-align: left; margin-bottom: 20px;">
                <tr style="background-color: #fff3e0;">
                    <th style="padding:10px; border:1px solid #ddd;">Cell ID</th>
                    <th style="padding:10px; border:1px solid #ddd;">Dự báo RSRP</th>
                    <th style="padding:10px; border:1px solid #ddd;">Dự báo Tải</th>
                    <th style="padding:10px; border:1px solid #ddd;">Giờ & Rủi ro</th>
                </tr>
                {table_rows_fc}
            </table>
            """
            html_content += """
                <p>Vui lòng truy cập <b>Dashboard</b> để kiểm tra cấu trúc không gian và lập phương án xử lý.</p>
                <p style="margin-top: 30px; font-size: 0.8em; color: #777;"><i>Thông báo tự động từ G-SQM Alert System.</i></p>
            </div>
            """
        else:
            html_content = f"<h3>MDT Status: Normal</h3><p>Tại giờ {max_hour}:00 ngày {target_date}, mạng lưới hoạt động cực kì ổn định, không phát hiện vùng lõm sóng hay rủi ro tương lai.</p>"

        smtp_hook = SmtpHook(smtp_conn_id='mdt_smtp_connection')
        with smtp_hook.get_conn() as client:
            smtp_hook.send_email_smtp(
                to='maithanhduy1705@gmail.com', 
                subject=f'[G-SQM ALERT] Báo cáo Bất thường & Cảnh báo Sớm - {target_date} ({max_hour}:00)',
                html_content=html_content,
                from_email='thanhduy175.hust@gmail.com'
            )
        print("Gửi Email thành công!")

    except Exception as e:
        print(f"LỖI HỆ THỐNG: {e}")
        raise RuntimeError(f"Luồng gửi mail thất bại: {e}")
    finally:
        if conn: conn.close()

default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'mdt_alert_system_via_ui',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2025, 5, 11), 
    catchup=False,               
    tags=['hourly_trigger']
) as dag:

    task_monitor_and_alert = PythonOperator(
        task_id='monitor_network_quality',
        python_callable=check_and_send_alert,
    )