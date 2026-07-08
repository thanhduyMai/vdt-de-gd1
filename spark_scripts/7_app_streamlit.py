import streamlit as st
import streamlit.components.v1 as components
import trino
import pandas as pd
from keplergl import KeplerGl
import json
import base64
import datetime
import warnings
import os
import uuid
import numpy as np

warnings.filterwarnings('ignore', message='.*SQLAlchemy connectable.*')

st.set_page_config(page_title="G-SQM", layout="wide", page_icon="")
st.markdown("""
    <style>
        .main .block-container {
            max-width: 100% !important;
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
        }
        iframe {
            width: 100% !important;
        }
    </style>
""", unsafe_allow_html=True)

def get_trino_connection():
    return trino.dbapi.connect(
        host="mdt_trino", port=8080, user="admin", catalog="lakehouse", schema="default"
    )

def create_sector_geojson(df_cell):
    features = []
    radius = 0.005 
    for _, row in df_cell.iterrows():
        lat, lon, az = row['station_lat'], row['station_lng'], row['azimuth']
        angle1, angle2 = az - 30, az + 30
        p1 = [lon, lat]
        p2 = [lon + radius * np.sin(np.radians(angle1)), lat + radius * np.cos(np.radians(angle1))]
        p3 = [lon + radius * np.sin(np.radians(angle2)), lat + radius * np.cos(np.radians(angle2))]
        features.append({"type": "Feature", "properties": {"cell_id": row['cell_code']}, "geometry": {"type": "Polygon", "coordinates": [[p1, p2, p3, p1]]}})
    return {"type": "FeatureCollection", "features": features}

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_filter_options(date_str):
    conn = None
    try:
        conn = get_trino_connection()
        query_actual = f"SELECT DISTINCT h3_index, cell_id FROM lakehouse.default.fact_mdt_hourly WHERE date = '{date_str}'"
        df_actual = pd.read_sql_query(query_actual, conn)
        if not df_actual.empty:
            return df_actual
        else:
            query_forecast = f"SELECT DISTINCT h3_index, cell_id FROM lakehouse.default.fact_forecast_hourly WHERE CAST((CAST(forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) AS DATE) = DATE('{date_str}')"
            return pd.read_sql_query(query_forecast, conn)
    except Exception as e:
        print(f"Lỗi fetch filter: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

default_date = datetime.date.today() - datetime.timedelta(days=1)

url_date = st.query_params.get("date", default_date.strftime("%Y-%m-%d"))
try:
    current_date_val = datetime.datetime.strptime(url_date, "%Y-%m-%d").date()
except:
    current_date_val = default_date

selected_date = st.sidebar.date_input("Chọn Ngày", current_date_val, key="date_picker")
date_str = selected_date.strftime("%Y-%m-%d")

st.query_params["date"] = date_str
last_week_date_str = (datetime.datetime.strptime(date_str, "%Y-%m-%d") - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

df_filters = fetch_filter_options(date_str)

param_h3 = st.query_params.get("h3", "Toàn mạng lưới")
param_cell = st.query_params.get("cell", "Toàn mạng lưới")

st.sidebar.title("Bảng Điều Khiển")

if st.sidebar.button("Xem Toàn Mạng Lưới (Reset)", use_container_width=True):
    if "h3" in st.query_params: del st.query_params["h3"]
    if "cell" in st.query_params: del st.query_params["cell"]
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("Phân tích chi tiết (Cell-Level)")

if not df_filters.empty:
    list_h3 = ["Toàn mạng lưới"] + sorted([str(x) for x in df_filters['h3_index'].dropna().unique()])
    index_h3 = list_h3.index(param_h3) if param_h3 in list_h3 else 0
    selected_h3 = st.sidebar.selectbox("Chọn khu vực (H3 Index)", list_h3, index=index_h3, key="h3_key")
    
    if selected_h3 != "Toàn mạng lưới": st.query_params["h3"] = selected_h3
    else:
        if "h3" in st.query_params: del st.query_params["h3"]
    
    df_f = df_filters[df_filters['h3_index'] == selected_h3] if selected_h3 != "Toàn mạng lưới" else df_filters
    list_cell = ["Toàn mạng lưới"] + sorted([str(x) for x in df_f['cell_id'].dropna().unique()])
    index_cell = list_cell.index(param_cell) if param_cell in list_cell else 0
    selected_cell = st.sidebar.selectbox("Chọn Trạm (Cell ID)", list_cell, index=index_cell, key="cell_key")
    
    if selected_cell != "Toàn mạng lưới": st.query_params["cell"] = selected_cell
    else:
        if "cell" in st.query_params: del st.query_params["cell"]
else:
    selected_h3, selected_cell = "Toàn mạng lưới", "Toàn mạng lưới"
    st.sidebar.warning("Không tìm thấy danh sách trạm khả dụng cho ngày này.")

st.sidebar.markdown("---")
st.sidebar.subheader("Cấu hình hiển thị")

critical_hours = st.sidebar.slider("Ngưỡng Nghiêm Trọng (số giờ tồi liên tục):", min_value=1, max_value=12, value=3)

enable_comparison = st.sidebar.toggle("Bật So sánh 2 Bản đồ (Side-by-Side)", value=False)

qr_options = ["Giờ mới nhất", "Toàn bộ 24 giờ (Bật Timeline)", "Xem theo từng giờ (Đồng bộ giao diện)"]
qr_default = st.query_params.get("range", "Giờ mới nhất")
qr_idx = qr_options.index(qr_default) if qr_default in qr_options else 0
query_range = st.sidebar.radio("Phạm vi dữ liệu không gian:", qr_options, index=qr_idx)
st.query_params["range"] = query_range

selected_hour = None
if query_range == "Xem theo từng giờ (Đồng bộ giao diện)":
    # Tự động gán giờ mặc định dựa trên ngày được chọn (đỡ mất công kéo)
    if current_date_val == datetime.date.today():
        default_hr = max(0, datetime.datetime.now().hour - 1)  # Thường data update trễ 1h
    elif current_date_val < datetime.date.today():
        default_hr = 23
    else:
        default_hr = 0
    
    selected_hour = st.sidebar.slider("Chọn khung giờ phân tích:", min_value=0, max_value=23, value=default_hr, step=1, format="%02d:00")

mm_options = ["Thực tế (MDT Actual)", "Dự báo (Forecast)"]
mm_default = st.query_params.get("mode", "Thực tế (MDT Actual)")
mm_idx = mm_options.index(mm_default) if mm_default in mm_options else 0
map_mode = st.sidebar.radio("Nguồn dữ liệu không gian:", mm_options, index=mm_idx)
st.query_params["mode"] = map_mode

quality_options = ["Tất cả", "Tốt", "Bình thường", "Cần để ý (Sóng yếu)", "Cần tối ưu (Lõm sóng)", "Nghiêm trọng (Tồi liên tục)"]
selected_quality = st.sidebar.selectbox("Chọn mức chất lượng:", options=quality_options, index=0, key="quality_dropdown")

alert_mode = st.sidebar.toggle("Chỉ hiện khu vực RSRP < -120 dBm", value=st.query_params.get("alert", "false").lower() == "true")
st.query_params["alert"] = "true" if alert_mode else "false"

def format_spatial_df(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=['h3_index', 'cell_id', 'cell_lat', 'cell_lon', 'h3_center_lat', 'h3_center_lon', 'user_density', 'avg_rsrp', 'avg_distance_km', 'p10_rsrp', 'record_time', 'Health Status'])
    float_cols = ['cell_lat', 'cell_lon', 'h3_center_lat', 'h3_center_lon', 'avg_rsrp', 'p10_rsrp']
    for col in float_cols: 
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
    df['avg_distance_km'] = pd.to_numeric(df['avg_distance_km'], errors='coerce').fillna(0.0)
    df['user_density'] = pd.to_numeric(df['user_density'], errors='coerce').fillna(0).astype(int)
    df['h3_index'] = df['h3_index'].astype(str)
    df['record_time'] = pd.to_datetime(df['record_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def fetch_dashboard_data(date_str, selected_h3, selected_cell, map_mode, query_range, enable_comp, critical_hours, selected_hour):
    conn = None
    try:
        conn = get_trino_connection()
        h3_cond = f"AND f.h3_index = '{selected_h3}'" if selected_h3 != "Toàn mạng lưới" else ""
        cell_cond = f"AND f.cell_id = '{selected_cell}'" if selected_cell != "Toàn mạng lưới" else ""

        hour_filter = ""
        hour_cond_lw = ""
        ref_hour_query = "0"

        if query_range == "Giờ mới nhất":
            if map_mode == "Dự báo (Forecast)":
                ref_hour_query = f"SELECT MAX(EXTRACT(HOUR FROM (CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR))) FROM lakehouse.default.fact_forecast_hourly f WHERE CAST((CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) AS DATE) = DATE('{date_str}') {h3_cond} {cell_cond}"
                hour_filter = f"AND EXTRACT(HOUR FROM (CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR)) = ({ref_hour_query})"
                hour_cond_lw = f"AND CAST(f.hour AS INTEGER) = ({ref_hour_query})"
            else:
                ref_hour_query = f"SELECT MAX(CAST(f.hour AS INTEGER)) FROM lakehouse.default.fact_mdt_hourly f WHERE f.date = '{date_str}' {h3_cond} {cell_cond}"
                hour_filter = f"AND CAST(f.hour AS INTEGER) = ({ref_hour_query})"
                hour_cond_lw = f"AND CAST(f.hour AS INTEGER) = ({ref_hour_query})"
        elif query_range == "Xem theo từng giờ (Đồng bộ giao diện)" and selected_hour is not None:
            ref_hour_query = str(selected_hour)
            if map_mode == "Dự báo (Forecast)":
                hour_filter = f"AND EXTRACT(HOUR FROM (CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR)) = {ref_hour_query}"
            else:
                hour_filter = f"AND CAST(f.hour AS INTEGER) = {ref_hour_query}"
            hour_cond_lw = f"AND CAST(f.hour AS INTEGER) = {ref_hour_query}"

        query_last_week = f"SELECT SUM(user_density_count) AS lw_density, AVG(avg_rsrp) AS lw_rsrp FROM lakehouse.default.fact_mdt_hourly f WHERE f.date = '{last_week_date_str}' {h3_cond} {cell_cond} {hour_cond_lw}"

        query_spatial_lw = ""
        if map_mode == "Dự báo (Forecast)":
            pushdown_filter = "WHERE status_label NOT IN ('Bình thường', 'Tốt')" if query_range == "Toàn bộ 24 giờ (Bật Timeline)" else ""
            query_spatial = f"""
            WITH RawForecast AS (
                SELECT f.h3_index, f.cell_id, c.station_lat AS cell_lat, c.station_lng AS cell_lon, h.h3_center_lat, h.h3_center_lon, 
                    CAST(f.forecast_density AS INTEGER) AS user_density, f.forecast_rsrp AS avg_rsrp, 0.0 AS avg_distance_km, f.forecast_rsrp_lower AS p10_rsrp, 
                    EXTRACT(HOUR FROM (CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR)) AS hour_num, 
                    (CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) AS record_time,
                    CASE WHEN (f.forecast_rsrp - f.forecast_rsrp_lower) > 15 AND f.forecast_density >= 50 THEN 'Cần tối ưu (Lõm sóng)' WHEN f.forecast_rsrp_upper < -100 THEN 'Cần để ý (Sóng yếu)' WHEN f.forecast_rsrp >= -90 THEN 'Tốt' ELSE 'Bình thường' END AS status_label,
                    f.forecast_rsrp, f.forecast_rsrp_lower, f.forecast_upper
                FROM lakehouse.default.fact_forecast_hourly f
                LEFT JOIN lakehouse.default.dim_cell c ON f.cell_id = UPPER(TRIM(c.cell_code)) 
                LEFT JOIN lakehouse.default.dim_h3 h ON f.h3_index = h.h3_index 
                WHERE CAST((CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) AS DATE) = DATE('{date_str}') {h3_cond} {cell_cond} {hour_filter}
            )
            SELECT h3_index, cell_id, cell_lat, cell_lon, h3_center_lat, h3_center_lon, user_density, avg_rsrp, avg_distance_km, p10_rsrp, hour_num, record_time, status_label AS "Health Status",
                   ROUND(forecast_rsrp, 2) AS "RSRP (Dự báo 1h tới)",
                   CASE 
                       WHEN (forecast_rsrp - forecast_rsrp_lower) > 10 AND user_density > (forecast_upper * 0.5) THEN 'Nguy cơ suy giảm/Lõm sóng'
                       WHEN forecast_rsrp < -100 THEN 'Dự báo tiếp tục lõm sóng'
                       ELSE 'An toàn'
                   END AS "Cảnh báo sớm"
            FROM RawForecast {pushdown_filter}
            """
        else:
            query_spatial = f"""
            WITH BaseData AS ( 
                SELECT f.h3_index, f.cell_id, c.station_lat AS cell_lat, c.station_lng AS cell_lon, h.h3_center_lat, h.h3_center_lon, f.user_density_count AS user_density, f.avg_rsrp, f.avg_distance_km, f.p10_rsrp, CAST(f.hour AS INTEGER) AS hour_num, 
                date_parse(f.date || ' ' || LPAD(CAST(f.hour AS VARCHAR), 2, '0') || ':00:00', '%Y-%m-%d %H:%i:%s') AS record_time  
                FROM lakehouse.default.fact_mdt_hourly f 
                LEFT JOIN lakehouse.default.dim_cell c ON f.cell_id = UPPER(TRIM(c.cell_code)) 
                LEFT JOIN lakehouse.default.dim_h3 h ON f.h3_index = h.h3_index 
                WHERE f.date = '{date_str}' {h3_cond} {cell_cond}
            ), CalculatedDiff AS ( 
                SELECT *, 
                user_density - LAG(user_density, 1, 0) OVER (PARTITION BY cell_id, h3_index ORDER BY hour_num ASC) AS density_diff,
                SUM(CASE WHEN avg_rsrp < -100 THEN 1 ELSE 0 END) OVER (PARTITION BY cell_id, h3_index ORDER BY hour_num ASC ROWS BETWEEN {critical_hours - 1} PRECEDING AND CURRENT ROW) AS bad_hours_count
                FROM BaseData 
            ) 
            SELECT d.h3_index, d.cell_id, d.cell_lat, d.cell_lon, d.h3_center_lat, d.h3_center_lon, d.user_density, d.avg_rsrp, d.avg_distance_km, d.p10_rsrp, d.record_time, 
            CASE 
                WHEN d.bad_hours_count >= {critical_hours} THEN 'Nghiêm trọng (Tồi liên tục)'
                WHEN d.avg_rsrp < -100 AND d.user_density >= 50 THEN 'Cần tối ưu (Lõm sóng)' 
                WHEN d.avg_rsrp < -100 AND d.density_diff < 0 THEN 'Cần để ý (Sóng yếu)' 
                WHEN d.avg_rsrp >= -90 THEN 'Tốt' 
                ELSE 'Bình thường' 
            END AS "Health Status",
            ROUND(f.forecast_rsrp, 2) AS "RSRP (Dự báo 1h tới)",
            CASE 
                WHEN f.forecast_rsrp IS NULL THEN 'Chưa có data'
                WHEN (f.forecast_rsrp - f.forecast_rsrp_lower) > 10 AND f.forecast_density > (f.forecast_upper * 0.5) THEN 'Nguy cơ suy giảm/Lõm sóng'
                WHEN f.forecast_rsrp < -100 THEN 'Dự báo tiếp tục lõm sóng'
                ELSE 'An toàn'
            END AS "Cảnh báo sớm"
            FROM CalculatedDiff d
            LEFT JOIN lakehouse.default.fact_forecast_hourly f
                ON d.cell_id = f.cell_id AND d.h3_index = f.h3_index 
                AND date_trunc('hour', CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) = date_add('hour', 1, d.record_time)
            { "WHERE d.hour_num = (" + ref_hour_query + ")" if query_range in ["Giờ mới nhất", "Xem theo từng giờ (Đồng bộ giao diện)"] else "" }
            """
            
            if enable_comp:
                query_spatial_lw = f"""
                WITH BaseData AS ( 
                    SELECT f.h3_index, f.cell_id, c.station_lat AS cell_lat, c.station_lng AS cell_lon, h.h3_center_lat, h.h3_center_lon, f.user_density_count AS user_density, f.avg_rsrp, f.avg_distance_km, f.p10_rsrp, CAST(f.hour AS INTEGER) AS hour_num, 
                    date_parse(f.date || ' ' || LPAD(CAST(f.hour AS VARCHAR), 2, '0') || ':00:00', '%Y-%m-%d %H:%i:%s') AS record_time  
                    FROM lakehouse.default.fact_mdt_hourly f 
                    LEFT JOIN lakehouse.default.dim_cell c ON f.cell_id = UPPER(TRIM(c.cell_code)) 
                    LEFT JOIN lakehouse.default.dim_h3 h ON f.h3_index = h.h3_index 
                    WHERE f.date = '{last_week_date_str}' {h3_cond} {cell_cond}
                ), CalculatedDiff AS ( 
                    SELECT *, 
                    user_density - LAG(user_density, 1, 0) OVER (PARTITION BY cell_id, h3_index ORDER BY hour_num ASC) AS density_diff,
                    SUM(CASE WHEN avg_rsrp < -100 THEN 1 ELSE 0 END) OVER (PARTITION BY cell_id, h3_index ORDER BY hour_num ASC ROWS BETWEEN {critical_hours - 1} PRECEDING AND CURRENT ROW) AS bad_hours_count
                    FROM BaseData 
                ) 
                SELECT h3_index, cell_id, cell_lat, cell_lon, h3_center_lat, h3_center_lon, user_density, avg_rsrp, avg_distance_km, p10_rsrp, record_time, 
                CASE 
                    WHEN bad_hours_count >= {critical_hours} THEN 'Nghiêm trọng (Tồi liên tục)'
                    WHEN avg_rsrp < -100 AND user_density >= 50 THEN 'Cần tối ưu (Lõm sóng)' 
                    WHEN avg_rsrp < -100 AND density_diff < 0 THEN 'Cần để ý (Sóng yếu)' 
                    WHEN avg_rsrp >= -90 THEN 'Tốt' 
                    ELSE 'Bình thường' 
                END AS "Health Status" 
                FROM CalculatedDiff
                { "WHERE hour_num = (" + ref_hour_query + ")" if query_range in ["Giờ mới nhất", "Xem theo từng giờ (Đồng bộ giao diện)"] else "" }
                """
        
        query_trend_history = f"SELECT date_parse(f.date || ' ' || LPAD(CAST(f.hour AS VARCHAR), 2, '0') || ':00:00', '%Y-%m-%d %H:%i:%s') AS record_time_dt, SUM(f.user_density_count) AS total_density, AVG(f.avg_rsrp) AS mean_rsrp, SUM(CASE WHEN f.avg_rsrp < -110 THEN 1 ELSE 0 END) AS poor_quality_cells FROM lakehouse.default.fact_mdt_hourly f WHERE f.date = '{date_str}' {h3_cond} {cell_cond} GROUP BY 1 ORDER BY 1"
        query_trend_forecast = f"SELECT date_trunc('hour', CAST(f.forecast_time AS TIMESTAMP)) AS record_time_dt, SUM(f.forecast_density) AS forecast_density, SUM(f.forecast_upper) AS forecast_upper, AVG(f.forecast_rsrp) AS mean_rsrp, AVG(f.forecast_rsrp_lower) AS lower_rsrp, AVG(f.forecast_rsrp_upper) AS upper_rsrp FROM lakehouse.default.fact_forecast_hourly f WHERE CAST((CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) AS DATE) = DATE('{date_str}') {h3_cond} {cell_cond} GROUP BY 1 ORDER BY 1"
        query_heatmap = f"SELECT hour, day_of_week(CAST(date AS DATE)) as dow_num, AVG(user_density_count) AS avg_density FROM lakehouse.default.fact_mdt_hourly f WHERE CAST(f.date AS DATE) BETWEEN CAST('{date_str}' AS DATE) - INTERVAL '28' DAY AND CAST('{date_str}' AS DATE) {h3_cond} {cell_cond} GROUP BY hour, day_of_week(CAST(date AS DATE))"
        query_risk = f"SELECT f.h3_index, h.h3_center_lat, h.h3_center_lon, f.cell_id, f.forecast_upper, f.forecast_rsrp, c.station_lat AS cell_lat, c.station_lng AS cell_lon, CASE WHEN (f.forecast_rsrp - f.forecast_rsrp_lower) > 10 AND f.forecast_density > (f.forecast_upper * 0.5) THEN 'Cảnh báo sớm: Lõm sóng / Suy hao 1h tới' ELSE 'Khác' END AS risk_type FROM lakehouse.default.fact_forecast_hourly f LEFT JOIN lakehouse.default.dim_cell c ON f.cell_id = UPPER(TRIM(c.cell_code)) LEFT JOIN lakehouse.default.dim_h3 h ON f.h3_index = h.h3_index WHERE CAST((CAST(f.forecast_time AS TIMESTAMP) + INTERVAL '7' HOUR) AS DATE) >= DATE('{date_str}') {h3_cond} {cell_cond} AND ((f.forecast_rsrp - f.forecast_rsrp_lower) > 10 AND f.forecast_density > (f.forecast_upper * 0.5))"
        query_metrics = f"SELECT cell_id as \"Mã Trạm (Cell ID)\", mae_density as \"MAE (User)\", mape_density as \"MAPE (User) %\", rmse_density as \"RMSE (User)\", mae_rsrp as \"MAE (RSRP dBm)\", rmse_rsrp as \"RMSE (RSRP dBm)\" FROM lakehouse.default.fact_model_evaluation WHERE eval_date = '{date_str}' {h3_cond} {cell_cond} ORDER BY mape_density DESC LIMIT 100"
        query_audit = f"SELECT * FROM lakehouse.default.quality_report WHERE date = '{date_str}' ORDER BY CAST(hour AS INTEGER) ASC"
        
        df_spatial = format_spatial_df(pd.read_sql_query(query_spatial, conn))
        df_spatial_lw = format_spatial_df(pd.read_sql_query(query_spatial_lw, conn)) if enable_comp else pd.DataFrame()
        df_history = pd.read_sql_query(query_trend_history, conn)
        df_forecast = pd.read_sql_query(query_trend_forecast, conn)
        df_hm = pd.read_sql_query(query_heatmap, conn)
        df_risk = pd.read_sql_query(query_risk, conn)
        df_lw = pd.read_sql_query(query_last_week, conn)
        
        try: df_metrics = pd.read_sql_query(query_metrics, conn)
        except: df_metrics = pd.DataFrame()
        try: df_audit = pd.read_sql_query(query_audit, conn)
        except: df_audit = pd.DataFrame() 

        return df_spatial, df_history, df_forecast, df_hm, df_risk, df_audit, df_metrics, df_lw, df_spatial_lw, None

    except Exception as e:
        return None, None, None, None, None, None, None, None, None, f"Thất bại khi xử lý dữ liệu: {e}"
    finally:
        if conn: conn.close()

def local_filter(df_spatial, selected_h3, selected_cell, selected_quality):
    if df_spatial is None or df_spatial.empty:
        return df_spatial
    if selected_h3 != "Toàn mạng lưới":
        df_spatial = df_spatial[df_spatial['h3_index'] == selected_h3]
    if selected_cell != "Toàn mạng lưới":
        df_spatial = df_spatial[df_spatial['cell_id'] == selected_cell]
    if selected_quality != "Tất cả":
        df_spatial = df_spatial[df_spatial['Health Status'].str.strip() == selected_quality.strip()]
    return df_spatial

chart_title_suffix = "Toàn mạng"
if selected_cell != "Toàn mạng lưới": chart_title_suffix = f"Trạm: {selected_cell}"
elif selected_h3 != "Toàn mạng lưới": chart_title_suffix = f"H3: {selected_h3}"

with st.spinner(f"Trino đang quét dữ liệu [{map_mode}] cho {chart_title_suffix}..."):
    df_spatial_full, df_history, df_forecast, df_hm, df_risk, df_audit, df_metrics, df_lw, df_spatial_lw_full, err = fetch_dashboard_data(date_str, selected_h3, selected_cell, map_mode, query_range, enable_comparison, critical_hours, selected_hour)
    df_spatial = local_filter(df_spatial_full, selected_h3, selected_cell, selected_quality)
    if enable_comparison:
        df_spatial_lw = local_filter(df_spatial_lw_full, selected_h3, selected_cell, selected_quality)

if err: st.error(err); st.stop()
if df_spatial is None: st.error("Không thể kết nối hoặc lấy dữ liệu từ hệ thống."); st.stop()
if df_spatial.empty and (df_forecast is None or df_forecast.empty): st.warning(f"Phân vùng ngày {date_str} không có dữ liệu thực tế lẫn dữ liệu dự báo."); st.stop()
if alert_mode and not df_spatial.empty: df_spatial = df_spatial[df_spatial['avg_rsrp'] < -120]

st.header(f"Báo cáo Vùng Phủ MDT - {date_str} [{map_mode}]")
c1, c2, c3 = st.columns(3)

st.markdown("---")
if not df_spatial.empty:
    nghiem_trong_df = df_spatial[df_spatial['Health Status'].str.contains('Nghiêm trọng', case=False, na=False)]
    lom_song_df = df_spatial[df_spatial['Health Status'].str.contains('lõm sóng|Cần tối ưu', case=False, na=False)]
    
    if nghiem_trong_df.shape[0] > 0:
        st.error(f"**BÁO ĐỘNG ĐỎ:** {nghiem_trong_df.shape[0]} phân vùng lõm sóng liên tục trong **{critical_hours} giờ**! Yêu cầu xử lý kỹ thuật lập tức.")
    elif lom_song_df.shape[0] > 0:
        st.warning(f"Phát hiện **{lom_song_df.shape[0]} phân vùng** rơi vào lõm sóng. Cần tối ưu!")
    else:
        st.success("Mạng lưới khu vực này đang ổn định: Không phát hiện vùng lõm sóng hay tồi liên tục.")

# --- BẢNG CẢNH BÁO SỚM TRÊN GIAO DIỆN ---
# --- BẢNG CẢNH BÁO SỚM TRÊN GIAO DIỆN ---
st.subheader("Cảnh báo sớm (Dự báo rủi ro toàn mạng)")

# Cho Text đếm thẳng từ df_risk để khớp 100% với số chấm vàng trên Map
if df_risk is not None and not df_risk.empty:
    st.warning(f"**FORECAST:** Cảnh báo có **{len(df_risk)} khu vực** có nguy cơ rớt chất lượng/lõm sóng trong 1 giờ tới!")
    
elif df_spatial_full is not None and "Cảnh báo sớm" in df_spatial_full.columns and (df_spatial_full["Cảnh báo sớm"] == 'Chưa có data').all():
    st.info("Trạng thái : Chưa có dữ liệu dự báo (Hãy check lại data pipeline).")
    
else:
    st.success("**FORECAST:** Mạng lưới an toàn, dự báo 0 khu vực lõm sóng.")

total_users, avg_rsrp_overall, current_hour = 0, 0.0, "N/A"
time_label, delta_density_str, delta_rsrp_str = "Khung giờ", None, None

if not df_spatial.empty:
    total_users = df_spatial['user_density'].sum()
    avg_rsrp_overall = df_spatial['avg_rsrp'].mean()
    if query_range == "Giờ mới nhất":
        current_hour = pd.to_datetime(df_spatial['record_time'].max()).strftime('%H:%M')
        time_label = "Giờ quét mới nhất"
    elif query_range == "Xem theo từng giờ (Đồng bộ giao diện)":
        current_hour = f"{selected_hour:02d}:00"
        time_label = "Giờ phân tích"
    else:
        current_hour = "Toàn bộ 24H"
        time_label = "Khung giờ hiển thị"

    if df_lw is not None and not df_lw.empty:
        lw_density = df_lw['lw_density'].iloc[0] if pd.notna(df_lw['lw_density'].iloc[0]) else 0
        lw_rsrp = df_lw['lw_rsrp'].iloc[0] if pd.notna(df_lw['lw_rsrp'].iloc[0]) else 0.0
        if lw_density > 0: delta_density_str = f"{int(total_users - lw_density):,} (tuần trước)"
        if lw_rsrp != 0.0: delta_rsrp_str = f"{avg_rsrp_overall - lw_rsrp:.1f} dBm (tuần trước)"

with c2: st.metric(time_label, current_hour) 
with c1: st.metric("Tổng thiết bị (User Density)", f"{total_users:,}", delta=delta_density_str)
with c3: st.metric("RSRP Trung bình hiển thị", f"{avg_rsrp_overall:.1f} dBm" if total_users > 0 else "N/A", delta=delta_rsrp_str)

df_alerts = df_risk.rename(columns={'forecast_upper': 'user_density', 'forecast_rsrp': 'avg_rsrp', 'risk_type': 'Health Status'}) if df_risk is not None and not df_risk.empty else pd.DataFrame()

if df_forecast is not None and not df_forecast.empty:
    df_forecast['record_time_dt'] = pd.to_datetime(df_forecast['record_time_dt'])
    df_forecast_filtered = df_forecast[df_forecast['record_time_dt'].dt.strftime('%Y-%m-%d') == date_str]
else: df_forecast_filtered = pd.DataFrame()

if df_history is not None and not df_history.empty:
    df_history['record_time_dt'] = pd.to_datetime(df_history['record_time_dt'])
    df_history['is_forecast'] = False
    df_history['upper_density'] = df_history['total_density'] 
    df_history['lower_rsrp'] = df_history['mean_rsrp']
    df_history['upper_rsrp'] = df_history['mean_rsrp']
    if not df_forecast_filtered.empty:
        last_time = df_history['record_time_dt'].max()
        df_forecast_remaining = df_forecast_filtered[df_forecast_filtered['record_time_dt'] > last_time].copy()
        if not df_forecast_remaining.empty:
            df_forecast_remaining['poor_quality_cells'] = df_history['poor_quality_cells'].iloc[-1]
            df_forecast_remaining['is_forecast'] = True
            df_forecast_remaining = df_forecast_remaining.rename(columns={'forecast_density': 'total_density', 'forecast_upper': 'upper_density'})
            combined_trend = pd.concat([df_history, df_forecast_remaining], ignore_index=True)
        else: combined_trend = df_history.copy()
    else: combined_trend = df_history.copy()
else:
    if not df_forecast_filtered.empty:
        df_forecast_full = df_forecast_filtered.copy()
        df_forecast_full['poor_quality_cells'], df_forecast_full['is_forecast'] = 0, True
        df_forecast_full = df_forecast_full.rename(columns={'forecast_density': 'total_density', 'forecast_upper': 'upper_density'})
        combined_trend = df_forecast_full
    else: combined_trend = pd.DataFrame(columns=['record_time_dt', 'total_density', 'upper_density', 'mean_rsrp', 'lower_rsrp', 'upper_rsrp', 'poor_quality_cells', 'is_forecast'])

if not combined_trend.empty:
    combined_trend['hour_label'] = combined_trend['record_time_dt'].dt.strftime('%H:00')
    js_labels = json.dumps(combined_trend['hour_label'].tolist())
    js_density = json.dumps(combined_trend['total_density'].tolist())
    js_upper_density = json.dumps(combined_trend['upper_density'].round(0).tolist())
    js_rsrp = json.dumps(combined_trend['mean_rsrp'].round(2).tolist())
    js_rsrp_lower = json.dumps(combined_trend['lower_rsrp'].round(2).tolist())
    js_rsrp_upper = json.dumps(combined_trend['upper_rsrp'].round(2).tolist())
    js_poor_cells = json.dumps(combined_trend['poor_quality_cells'].tolist())
    js_is_forecast = json.dumps(combined_trend['is_forecast'].tolist())
else:
    js_labels, js_density, js_upper_density, js_rsrp, js_rsrp_lower, js_rsrp_upper, js_poor_cells, js_is_forecast = "[]", "[]", "[]", "[]", "[]", "[]", "[]", "[]"

dow_map = {1: "Thứ 2", 2: "Thứ 3", 3: "Thứ 4", 4: "Thứ 5", 5: "Thứ 6", 6: "Thứ 7", 7: "Chủ Nhật"}
heatmap_dict = {f"{h}_{d}": 0.0 for d in range(1, 8) for h in range(24)}
max_density = float(df_hm['avg_density'].max()) if not df_hm.empty and df_hm['avg_density'].max() > 0 else 1
if not df_hm.empty:
    for _, row in df_hm.iterrows():
        if int(row['dow_num']) in dow_map: heatmap_dict[f"{int(row['hour'])}_{int(row['dow_num'])}"] = float(row['avg_density'])
heatmap_list = [{"x": f"{h:02d}:00", "y": dow_map[d], "v": heatmap_dict[f"{h}_{d}"]} for d in range(1, 8) for h in range(24)]
js_heatmap_data = json.dumps(heatmap_list)

x_axis_labels = json.dumps([f"{i:02d}:00" for i in range(24)])
y_axis_labels = json.dumps(["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"])

esri_style = {
    "version": 8,
    "sources": {
        "esri-satellite": {"type": "raster", "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"], "tileSize": 256},
        "esri-labels": {"type": "raster", "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}"], "tileSize": 256}
    },
    "layers": [{"id": "satellite-layer", "type": "raster", "source": "esri-satellite", "minzoom": 0, "maxzoom": 22}, {"id": "labels-layer", "type": "raster", "source": "esri-labels", "minzoom": 0, "maxzoom": 22}]
}
style_data_uri = f"data:application/json;base64,{base64.b64encode(json.dumps(esri_style).encode('utf-8')).decode('utf-8')}"

legend_and_script_html = f"""
<style>
    html, body {{ width: 100% !important; height: 100vh !important; margin: 0 !important; overflow: hidden !important; }}
    .rsrp-legend-container {{ position: absolute; bottom: 35px; left: 50%; transform: translateX(-50%); display: flex; flex-direction: row; box-shadow: 0 6px 12px rgba(0,0,0,0.4); border: 2px solid rgba(255,255,255,0.8); border-radius: 6px; overflow: hidden; z-index: 10000; }}
    .rsrp-box {{ display: flex; flex-direction: column; align-items: center; justify-content: center; width: 100px; height: 50px; font-family: 'Segoe UI', sans-serif; font-size: 12px; font-weight: bold; text-align: center; line-height: 1.2; }}
    .c-1 {{ background-color: #d73027; color: #fff; }} .c-2 {{ background-color: #fc8d59; color: #000; }} .c-3 {{ background-color: #fee08b; color: #000; }} .c-4 {{ background-color: #d9ef8b; color: #000; }} .c-5 {{ background-color: #91cf60; color: #000; }} .c-6 {{ background-color: #1a9850; color: #fff; }}
</style>
<div class="rsrp-legend-container">
    <div class="rsrp-box c-1"><span>Rất Kém</span><span>&lt; -110 dBm</span></div><div class="rsrp-box c-2"><span>Kém</span><span>-110 ÷ -105</span></div><div class="rsrp-box c-3"><span>TB-Yếu</span><span>-105 ÷ -100</span></div><div class="rsrp-box c-4"><span>Trung Bình</span><span>-100 ÷ -95</span></div><div class="rsrp-box c-5"><span>Khá</span><span>-95 ÷ -85</span></div><div class="rsrp-box c-6"><span>Tốt</span><span>&gt; -85 dBm</span></div>
</div>
<script>
document.addEventListener('click', function(event) {{
    if (event.target.closest('#insight-panel') || event.target.closest('.rsrp-legend-container')) return;
    let attempts = 0;
    const checkTooltip = setInterval(() => {{
        attempts++;
        let h3 = '', cell = '', foundTooltip = false;
        const rows = document.querySelectorAll('table tbody tr');
        if (rows.length > 0) {{
            foundTooltip = true;
            rows.forEach(row => {{
                const cells = row.querySelectorAll('td');
                if (cells.length >= 2) {{
                    const name = (cells[0].textContent || "").trim();
                    const value = (cells[1].textContent || "").trim();
                    if (name === 'h3_index') h3 = value;
                    if (name === 'cell_id') cell = value;
                }}
            }});
        }}
        if (foundTooltip) {{
            clearInterval(checkTooltip);
            if (h3 || cell) {{
                try {{
                    const targetWindow = window.parent;
                    const url = new URL(targetWindow.location.href);
                    let changed = false;
                    if (h3 && url.searchParams.get('h3') !== h3) {{ url.searchParams.set('h3', h3); changed = true; }}
                    if (cell && cell !== '' && url.searchParams.get('cell') !== cell) {{ url.searchParams.set('cell', cell); changed = true; }}
                    if (changed) targetWindow.location.search = url.search;
                }} catch (e) {{ console.error("Lỗi URL:", e); }}
            }}
        }} else if (attempts >= 10) clearInterval(checkTooltip);
    }}, 100);
}});
</script>
"""

insight_html = f"""
<style>
    #insight-panel {{ position: absolute; top: 15px; right: 15px; width: 420px; background: rgba(18, 20, 22, 0.9); backdrop-filter: blur(8px); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; color: #e2e8f0; z-index: 9999; box-shadow: 0 10px 25px rgba(0,0,0,0.5); font-family: 'Segoe UI', sans-serif; }}
    .panel-header {{ display: flex; justify-content: space-between; align-items: center; padding: 15px 20px; border-bottom: 1px solid rgba(255,255,255,0.1); }}
    .insight-title {{ font-size: 16px; margin: 0; font-weight: bold; color: #fff; }}
    #toggle-panel-btn {{ background: none; border: none; color: #a0aec0; font-size: 18px; cursor: pointer; }}
    #panel-content {{ padding: 0 20px 20px 20px; max-height: 80vh; overflow-y: auto; }}
    .chart-container {{ margin-top: 20px; }}
</style>
<div id="insight-panel">
    <div class="panel-header"><h3 class="insight-title">Network Insights</h3><button id="toggle-panel-btn">➖</button></div>
    <div id="panel-content">
        <div class="chart-container"><canvas id="chartHeatmap" height="150"></canvas></div><div class="chart-container"><canvas id="chartPoor"></canvas></div><div class="chart-container"><canvas id="chartDensity"></canvas></div><div class="chart-container"><canvas id="chartRsrp"></canvas></div>
    </div>
</div>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-chart-matrix@2.0.1/dist/chartjs-chart-matrix.min.js"></script>
<script>
    const toggleBtn = document.getElementById('toggle-panel-btn'); const panelContent = document.getElementById('panel-content'); let isPanelOpen = true;
    toggleBtn.addEventListener('click', () => {{ isPanelOpen = !isPanelOpen; panelContent.style.display = isPanelOpen ? 'block' : 'none'; toggleBtn.innerText = isPanelOpen ? '➖' : '➕'; }});
    const labels = {js_labels}; const isForecast = {js_is_forecast};
    const pointStyles = isForecast.map(f => f ? 'triangle' : 'circle'); 
    const segmentStyles = ctx => isForecast[ctx.p1DataIndex] ? [5, 5] : undefined;
    const commonOptions = {{ responsive: true, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ ticks: {{ color: '#a0aec0' }} }}, y: {{ ticks: {{ color: '#a0aec0' }} }} }} }};

    new Chart(document.getElementById('chartHeatmap'), {{ type: 'matrix', data: {{ datasets: [{{ label: 'Tải hệ thống', data: {js_heatmap_data}, backgroundColor: ctx => {{ if (!ctx.dataset.data[ctx.dataIndex]) return 'transparent'; const v = ctx.dataset.data[ctx.dataIndex].v; if (v === 0) return 'rgba(255, 255, 255, 0.05)'; const pct = v / {max_density}; if (pct >= 0.8) return '#081d58'; if (pct >= 0.6) return '#225ea8'; if (pct >= 0.4) return '#41b6c4'; if (pct >= 0.2) return '#c7e9b4'; return '#ffffcc'; }}, borderColor: 'rgba(255,255,255,0.05)', borderWidth: 1, width: c => (c.chartArea ? c.chartArea.width : 300) / 24 - 1, height: c => (c.chartArea ? c.chartArea.height : 100) / 7 - 1 }}] }}, options: {{ maintainAspectRatio: false, plugins: {{ title: {{ display: true, text: 'Ma trận tải tuần hoàn ({chart_title_suffix})', color: '#fff' }} }}, scales: {{ x: {{ type: 'category', labels: {x_axis_labels}, ticks: {{ color: '#a0aec0', maxTicksLimit: 8 }} }}, y: {{ type: 'category', labels: {y_axis_labels}, ticks: {{ color: '#a0aec0' }} }} }} }} }});
    new Chart(document.getElementById('chartPoor'), {{ type: 'bar', data: {{ labels: labels, datasets: [{{ label: 'Khu vực kém', data: {js_poor_cells}, backgroundColor: isForecast.map(f => f ? 'rgba(245, 101, 101, 0.4)' : '#f56565') }}] }}, options: {{ ...commonOptions, plugins: {{ title: {{ display: true, text: 'Vùng chất lượng kém 24H ({chart_title_suffix})', color: '#fff' }} }} }} }});
    new Chart(document.getElementById('chartDensity'), {{ type: 'line', data: {{ labels: labels, datasets: [{{ label: 'Rủi ro bùng phát tối đa (Upper Bound)', data: {js_upper_density}, borderColor: 'rgba(245, 101, 101, 0.8)', backgroundColor: 'transparent', pointRadius: 0, borderDash: [3, 3], borderWidth: 2, tension: 0.3 }}, {{ label: 'User Density', data: {js_density}, borderColor: '#38b2ac', backgroundColor: 'rgba(56, 178, 172, 0.2)', fill: true, pointStyle: pointStyles, tension: 0.3, segment: {{ borderDash: segmentStyles }} }}] }}, options: {{ ...commonOptions, plugins: {{ title: {{ display: true, text: 'Biến động User Density 24H ({chart_title_suffix})', color: '#fff' }} }} }} }});
    new Chart(document.getElementById('chartRsrp'), {{ type: 'line', data: {{ labels: labels, datasets: [{{ label: 'Biên độ nhiễu tối đa (Upper RSRP)', data: {js_rsrp_upper}, borderColor: 'rgba(236, 201, 75, 0.4)', backgroundColor: 'transparent', pointRadius: 0, borderDash: [3, 3], borderWidth: 1, tension: 0.3 }}, {{ label: 'Rủi ro suy hao sóng (Lower RSRP)', data: {js_rsrp_lower}, borderColor: 'rgba(236, 201, 75, 0.4)', backgroundColor: 'rgba(236, 201, 75, 0.1)', fill: '-1', pointRadius: 0, borderDash: [3, 3], borderWidth: 1, tension: 0.3 }}, {{ label: 'Avg RSRP', data: {js_rsrp}, borderColor: '#ecc94b', backgroundColor: 'transparent', pointStyle: pointStyles, tension: 0.3, segment: {{ borderDash: segmentStyles }} }}] }}, options: {{ ...commonOptions, plugins: {{ title: {{ display: true, text: 'Dải rủi ro RSRP 24H ({chart_title_suffix})', color: '#fff' }} }} }} }});
</script>
"""

@st.cache_data(show_spinner=False, ttl=3600)
def render_map_html(df_to_render, selected_h3, selected_cell, query_range, df_audit_data, df_alerts_data, legend_html, insight_html_str, style_uri, height=900, include_insights=False):
    kepler_filters = []
    df_map = df_to_render.copy()
    
    m_lat, m_lon, m_zoom = 21.5928, 105.8442, 11.5
    if not df_map.empty:
        if selected_cell != "Toàn mạng lưới" or selected_h3 != "Toàn mạng lưới":
            m_lat = df_map['cell_lat'].mean() if selected_cell != "Toàn mạng lưới" else df_map['h3_center_lat'].mean()
            m_lon = df_map['cell_lon'].mean() if selected_cell != "Toàn mạng lưới" else df_map['h3_center_lon'].mean()
            m_zoom = 14.5  

        if query_range == "Toàn bộ 24 giờ (Bật Timeline)":
            df_map['record_time_dt'] = pd.to_datetime(df_map['record_time'])
            min_dt, max_dt = df_map['record_time_dt'].min(), df_map['record_time_dt'].max()
            w_end = min_dt + pd.Timedelta(hours=1)
            if w_end > max_dt: w_end = max_dt
            
            kepler_filters.append({
                "dataId": ["MDT_Coverage_Live", "MDT_Coverage_Full", "Highlight_Holes"], 
                "id": "time-filter", "name": ["record_time"], "type": "timeRange", "enlarged": True,
                "value": [int(min_dt.timestamp() * 1000), int(w_end.timestamp() * 1000)]
            })
            df_map = df_map.drop(columns=['record_time_dt'], errors='ignore')

    map_config = {
        "version": "v1",
        "config": {
            "visState": {
                "filters": kepler_filters,
                "layers": [
                    {"id": "h3-hexagon-layer", "type": "hexagonId", "config": {"dataId": "MDT_Coverage_Live", "columns": {"hex_id": "h3_index"}, "colorField": {"name": "avg_rsrp", "type": "real"}, "colorScale": "custom", "sizeField": {"name": "user_density", "type": "integer"}, "sizeScale": "linear", "pickable": True, "isVisible": True, "visConfig": {"opacity": 0.8, "coverage": 0.95, "enable3d": True, "elevationScale": 0, "sizeRange": [0, 500], "colorRange": {"name": "Custom", "type": "custom", "category": "Custom", "colors": ["#d73027", "#fc8d59", "#fee08b", "#d9ef8b", "#91cf60", "#1a9850"]}, "customColorDomain": [-110, -100, -90, -80, -70]}}},
                    {"id": "cell-point-layer", "type": "point", "config": {"dataId": "MDT_Coverage_Live", "columns": {"lat": "cell_lat", "lng": "cell_lon"}, "color": [255, 255, 255], "isVisible": True, "visConfig": {"radius": 10}}},
                    {"id": "azimuth-sector-layer", "type": "geojson", "config": {"dataId": "Cell_Azimuth", "isVisible": True, "visConfig": {"filled": True, "opacity": 0.3}}},
                    {"id": "arc-signal-layer", "type": "arc", "config": {"dataId": "MDT_Coverage_Full", "color": [248, 149, 112], "columns": {"lat0": "cell_lat", "lng0": "cell_lon", "lat1": "h3_center_lat", "lng1": "h3_center_lon"}, "isVisible": True, "visConfig": {"opacity": 0.5, "thickness": 1.5, "targetColor": [128, 0, 0]}}},
                    {"id": "highlight-holes-layer", "type": "hexagonId", "config": {"dataId": "Highlight_Holes", "columns": {"hex_id": "h3_index"}, "color": [255, 0, 0], "isVisible": True, "visConfig": {"opacity": 1.0, "coverage": 1.0, "enable3d": True, "elevationScale": 0, "sizeField": {"name": "user_density", "type": "integer"}, "sizeScale": "linear", "sizeRange": [0, 500]}}},
                    {"id": "early-warning-layer", "type": "hexagonId", "config": {"dataId": "Alert_Stations", "columns": {"hex_id": "h3_index"}, "sizeField": {"name": "user_density", "type": "integer"}, "sizeScale": "linear", "color": [255, 255, 0], "isVisible": True, "visConfig": {"opacity": 0.8, "coverage": 0.7, "enable3d": True, "elevationScale": 15, "sizeRange": [0, 500]}}}
                ]
            },
            "mapState": {"latitude": m_lat, "longitude": m_lon, "zoom": m_zoom, "pitch": 45, "bearing": 0},
            "mapStyle": {"styleType": "esri_satellite", "mapStyles": {"esri_satellite": {"id": "esri_satellite", "label": "ESRI Satellite", "url": style_uri, "custom": True}}}
        }
    }

    m = KeplerGl(height=height, config=map_config)
    
    if not df_map.empty:
        mask_lom_song = df_map['Health Status'].str.contains('lõm sóng|Cần tối ưu|Nghiêm trọng', case=False, na=False)
        if 'Cảnh báo sớm' in df_map.columns:
            mask_lom_song = mask_lom_song | df_map['Cảnh báo sớm'].str.contains('Nguy cơ|tiếp tục lõm sóng', case=False, na=False)
            
        m.add_data(data=df_map[~mask_lom_song], name="MDT_Coverage_Live")
        m.add_data(data=df_map, name="MDT_Coverage_Full")
        if not df_map[mask_lom_song].empty: m.add_data(data=df_map[mask_lom_song], name="Highlight_Holes")
    else:
        m.add_data(data=pd.DataFrame(), name="MDT_Coverage_Live")
        m.add_data(data=pd.DataFrame(), name="MDT_Coverage_Full")

    if include_insights:
        if df_audit_data is not None and not df_audit_data.empty: m.add_data(data=df_audit_data, name="Quality_Audit_Report")
        if df_alerts_data is not None and not df_alerts_data.empty: m.add_data(data=df_alerts_data, name="Alert_Stations")

    temp_path = f"/tmp/kepler_{uuid.uuid4().hex}.html"
    m.save_to_html(file_name=temp_path, read_only=False)
    with open(temp_path, "r", encoding="utf-8") as f: html_content = f.read()
    os.remove(temp_path)

    html_content = html_content.replace("const store = ", "const store = window.keplerStore = ")
    
    injection = legend_html + (insight_html_str if include_insights else "")
    parts = html_content.rsplit("</body>", 1)
    return f"{parts[0]}{injection}</body>{parts[1]}"

if enable_comparison:
    col_map1, col_map2 = st.columns(2)
    with col_map1:
        st.markdown(f"**Ngày {date_str}**")
        components.html(
            render_map_html(df_spatial, selected_h3, selected_cell, query_range, df_audit, df_alerts, legend_and_script_html, insight_html, style_data_uri, height=750, include_insights=True), 
            height=750, scrolling=True
        )
    with col_map2:
        st.markdown(f"**Tuần trước ({last_week_date_str})**")
        components.html(
            render_map_html(df_spatial_lw, selected_h3, selected_cell, query_range, pd.DataFrame(), pd.DataFrame(), legend_and_script_html, insight_html, style_data_uri, height=750, include_insights=False), 
            height=750, scrolling=True
        )
else:
    components.html(
        render_map_html(df_spatial, selected_h3, selected_cell, query_range, df_audit, df_alerts, legend_and_script_html, insight_html, style_data_uri, height=900, include_insights=True), 
        height=900, scrolling=True
    )
st.markdown("---")
with st.expander("Dành cho Data Engineer: Báo cáo Vận hành Pipeline & Metrics", expanded=False):
    tab_risk_focus, tab_metrics, tab_forecast_details, tab_quality_audit = st.tabs([
        "Cảnh Báo Thuật Toán (df_risk)", 
        "Đánh giá Mô hình (Model Metrics)", 
        "Dữ liệu Dự báo (Forecast Detail)", 
        "Kiểm định Data (Quality Report)"
    ])

    with tab_risk_focus:
        if df_risk is not None and not df_risk.empty:
            df_risk_display = df_risk[['cell_id', 'forecast_upper', 'forecast_rsrp', 'risk_type', 'cell_lat', 'cell_lon']].copy()
            df_risk_display.columns = ['Mã Trạm', 'Dự báo Tải', 'RSRP Dự Báo', 'Phân Loại', 'Lat', 'Lon']
            st.dataframe(df_risk_display.style.format({"Dự báo Tải": "{:,}", "RSRP Dự Báo": "{:.2f} dBm"}), use_container_width=True)
        else:
            st.info("Hệ thống không ghi nhận bất thường.")

    with tab_metrics:
        if df_metrics is not None and not df_metrics.empty:
            st.dataframe(df_metrics.style.format("{:.2f}", subset=["MAE (User)", "RMSE (User)", "MAPE (User) %", "MAE (RSRP dBm)", "RMSE (RSRP dBm)"]), use_container_width=True)

    with tab_forecast_details:
        if df_forecast is not None and not df_forecast.empty:
            df_fc = df_forecast.copy()
            if 'record_time_dt' in df_fc.columns:
                df_fc['Khung Giờ'] = df_fc['record_time_dt'].dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(df_fc[['Khung Giờ', 'forecast_density', 'forecast_upper', 'mean_rsrp']].style.format({"forecast_density": "{:.1f}", "forecast_upper": "{:.1f}", "mean_rsrp": "{:.2f} dBm"}), use_container_width=True)
        else: 
            st.info("Không có dữ liệu dự báo.")

    with tab_quality_audit:
        if df_audit is not None and not df_audit.empty:
            st.dataframe(df_audit, use_container_width=True)
        else: 
            st.info("Không có báo cáo Audit.")