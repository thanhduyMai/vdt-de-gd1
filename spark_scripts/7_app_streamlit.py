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

warnings.filterwarnings('ignore', message='.*SQLAlchemy connectable.*')

st.set_page_config(page_title="MDT Coverage Live", layout="wide", page_icon="📡")

# =========================================================================
# KẾT NỐI TRINO DÙNG CHUNG
# =========================================================================
def get_trino_connection():
    # Nhớ đổi thành "localhost" nếu chạy Streamlit từ ngoài máy tính/Docker
    return trino.dbapi.connect(
        host="mdt_trino", port=8080, user="admin", catalog="lakehouse", schema="default"
    )

# =========================================================================
# TỐI ƯU 1: LẤY DANH SÁCH BỘ LỌC ĐỘC LẬP (Cực nhẹ)
# =========================================================================
#@st.cache_data(ttl=3600, show_spinner=False)
def fetch_filter_options(date_str):
    conn = None
    try:
        conn = get_trino_connection()
        query = f"SELECT DISTINCT h3_index, cell_id FROM lakehouse.default.fact_mdt_hourly WHERE date = '{date_str}'"
        return pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Lỗi fetch filter: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# =========================================================================
# TỐI ƯU 2: ĐẨY 100% LOGIC AGGREGATION & TOÁN HỌC XUỐNG TRINO
# =========================================================================
#@st.cache_data(ttl=3600, show_spinner=False)
def fetch_dashboard_data(date_str, selected_h3, selected_cell):
    conn = None
    try:
        conn = get_trino_connection()
        
        # Chèn điều kiện lọc động thẳng vào SQL (Pushdown Filter)
        h3_cond = f"AND f.h3_index = '{selected_h3}'" if selected_h3 != "Toàn mạng lưới" else ""
        cell_cond = f"AND f.cell_id = '{selected_cell}'" if selected_cell != "Toàn mạng lưới" else ""

        # 1. SQL Kéo dữ liệu không gian cho Bản đồ
        query_spatial = f"""
        WITH BaseData AS ( 
            SELECT f.h3_index, f.cell_id, c.station_lat AS cell_lat, c.station_lng AS cell_lon, c.bandwidth, h.h3_center_lat, h.h3_center_lon, f.user_density_count AS user_density, f.avg_rsrp, f.avg_distance_km, f.p10_rsrp, CAST(f.hour AS INTEGER) AS hour_num, 
            date_parse(f.date || ' ' || LPAD(CAST(f.hour AS VARCHAR), 2, '0') || ':00:00', '%Y-%m-%d %H:%i:%s') AS record_time 
            FROM lakehouse.default.fact_mdt_hourly f 
            LEFT JOIN lakehouse.default.dim_cell c ON f.cell_id = UPPER(TRIM(c.cell_code)) 
            LEFT JOIN lakehouse.default.dim_h3 h ON f.h3_index = h.h3_index 
            WHERE f.date = '{date_str}'
        ), CalculatedDiff AS ( 
            SELECT *, user_density - LAG(user_density, 1, 0) OVER (PARTITION BY cell_id, h3_index ORDER BY hour_num ASC) AS density_diff 
            FROM BaseData 
        ) 
        SELECT h3_index, cell_id, cell_lat, cell_lon, bandwidth, h3_center_lat, h3_center_lon, user_density, avg_rsrp, avg_distance_km, p10_rsrp, record_time, 
        CASE 
            WHEN avg_rsrp < -100 AND user_density >= 50 THEN ' Cần tối ưu' 
            WHEN avg_rsrp < -100 AND density_diff < 0 THEN ' Cần để ý' 
            WHEN avg_rsrp >= -70 AND density_diff < 0 THEN ' Bất thường' 
            WHEN avg_rsrp >= -90 THEN ' Tốt' 
            ELSE ' Bình thường' 
        END AS "Health Status" 
        FROM CalculatedDiff
        """
        
        # 2. SQL Kéo dữ liệu Lịch sử
        query_trend_history = f"""
            SELECT 
                date_parse(f.date || ' ' || LPAD(CAST(f.hour AS VARCHAR), 2, '0') || ':00:00', '%Y-%m-%d %H:%i:%s') AS record_time_dt,
                SUM(f.user_density_count) AS total_density,
                AVG(f.avg_rsrp) AS mean_rsrp,
                SUM(CASE WHEN f.avg_rsrp < -110 THEN 1 ELSE 0 END) AS poor_quality_cells
            FROM lakehouse.default.fact_mdt_hourly f
            WHERE f.date = '{date_str}' {h3_cond} {cell_cond}
            GROUP BY 1 ORDER BY 1
        """

        # 3. SQL Kéo dữ liệu Dự báo Đa biến (RSRP + Density + Dải rủi ro)
        query_trend_forecast = f"""
            SELECT 
                CAST(f.forecast_time AS TIMESTAMP) AS record_time_dt,
                SUM(f.forecast_density) AS forecast_density,
                SUM(f.forecast_upper) AS forecast_upper,
                AVG(f.forecast_rsrp) AS mean_rsrp,
                AVG(f.forecast_rsrp_lower) AS lower_rsrp,
                AVG(f.forecast_rsrp_upper) AS upper_rsrp
            FROM lakehouse.default.fact_forecast_hourly f
            WHERE CAST(f.forecast_date AS DATE) >= CAST('{date_str}' AS DATE)
            {h3_cond} {cell_cond}
            GROUP BY 1 ORDER BY 1
        """

        # 4. SQL Kéo Ma trận Heatmap
        query_heatmap = f"""
            SELECT hour, day_of_week(CAST(date AS DATE)) as dow_num, AVG(user_density_count) AS avg_density      
            FROM lakehouse.default.fact_mdt_hourly f
            WHERE CAST(f.date AS DATE) BETWEEN CAST('{date_str}' AS DATE) - INTERVAL '28' DAY AND CAST('{date_str}' AS DATE)
            {h3_cond} {cell_cond}
            GROUP BY hour, day_of_week(CAST(date AS DATE))
        """

        # 5. SQL Tính toán Toán học Shannon Alert
        query_risk = f"""
            SELECT COUNT(DISTINCT f.cell_id) as overload_risk_cells
            FROM lakehouse.default.fact_forecast_hourly f
            LEFT JOIN lakehouse.default.dim_cell c ON f.cell_id = UPPER(TRIM(c.cell_code))
            WHERE CAST(f.forecast_date AS DATE) >= CAST('{date_str}' AS DATE)
              AND f.forecast_upper > (COALESCE(c.bandwidth, 20) * 15)
              {h3_cond} {cell_cond}
        """

        query_audit = f"SELECT * FROM lakehouse.default.quality_report WHERE date = '{date_str}' ORDER BY CAST(hour AS INTEGER) ASC"
        
        df_spatial = pd.read_sql_query(query_spatial, conn)
        df_history = pd.read_sql_query(query_trend_history, conn)
        df_forecast = pd.read_sql_query(query_trend_forecast, conn)
        df_hm = pd.read_sql_query(query_heatmap, conn)
        df_risk = pd.read_sql_query(query_risk, conn)
        
        try: df_audit = pd.read_sql_query(query_audit, conn)
        except: df_audit = pd.DataFrame() 

        # Xử lý kiểu dữ liệu cơ bản cho Bản đồ
        if not df_spatial.empty:
            float_cols = ['cell_lat', 'cell_lon', 'h3_center_lat', 'h3_center_lon', 'avg_rsrp', 'p10_rsrp']
            for col in float_cols: 
                if col in df_spatial.columns: df_spatial[col] = pd.to_numeric(df_spatial[col], errors='coerce')
            df_spatial['avg_distance_km'] = pd.to_numeric(df_spatial['avg_distance_km'], errors='coerce').fillna(0.0)
            df_spatial['user_density'] = pd.to_numeric(df_spatial['user_density'], errors='coerce').fillna(0).astype(int)
            df_spatial['bandwidth'] = pd.to_numeric(df_spatial['bandwidth'], errors='coerce').fillna(20).astype(int)
            df_spatial['h3_index'] = df_spatial['h3_index'].astype(str)
            df_spatial['record_time'] = pd.to_datetime(df_spatial['record_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

        return df_spatial, df_history, df_forecast, df_hm, df_risk, df_audit, None

    except Exception as e:
        return None, None, None, None, None, None, f"Thất bại khi xử lý dữ liệu: {e}"
    finally:
        if conn:
            conn.close()

# =========================================================================
# GIAO DIỆN VÀ ĐIỀU KHIỂN (UI)
# =========================================================================
st.sidebar.title("🛠️ Bảng Điều Khiển")
default_date = datetime.date.today() - datetime.timedelta(days=1) 
selected_date = st.sidebar.date_input("📅 Chọn Ngày", default_date)
date_str = selected_date.strftime("%Y-%m-%d")

# 1. KÉO DANH SÁCH BỘ LỌC ĐỘC LẬP
df_filters = fetch_filter_options(date_str)

st.sidebar.markdown("---")
st.sidebar.subheader("🔎 Phân tích Cục bộ (Cell-Level)")

if not df_filters.empty:
    list_h3 = ["Toàn mạng lưới"] + sorted([str(x) for x in df_filters['h3_index'].dropna().unique()])
    selected_h3 = st.sidebar.selectbox("📍 Chọn khu vực (H3 Index)", list_h3, key="h3_key")

    if selected_h3 != "Toàn mạng lưới":
        df_filters = df_filters[df_filters['h3_index'] == selected_h3]

    list_cell = ["Toàn mạng lưới"] + sorted([str(x) for x in df_filters['cell_id'].dropna().unique()])
    selected_cell = st.sidebar.selectbox("🗼 Chọn Trạm (Cell ID)", list_cell, key="cell_key")
else:
    selected_h3, selected_cell = "Toàn mạng lưới", "Toàn mạng lưới"
    st.sidebar.warning("Không tìm thấy danh sách trạm. Vui lòng kiểm tra lại Data ca này.")

st.sidebar.markdown("---")
st.sidebar.subheader("🎯 Bộ lọc Hiển thị")
alert_mode = st.sidebar.toggle("🔴 Chỉ hiện khu vực RSRP < -100 dBm")

# Cập nhật Tiêu đề Biểu đồ
chart_title_suffix = "Toàn mạng"
if selected_cell != "Toàn mạng lưới": chart_title_suffix = f"Trạm: {selected_cell}"
elif selected_h3 != "Toàn mạng lưới": chart_title_suffix = f"H3: {selected_h3}"

# 2. GỌI TRUY VẤN CHÍNH
with st.spinner(f"Trino đang xử lý tính toán phân tán cho {chart_title_suffix}..."):
    df_spatial, df_history, df_forecast, df_hm, df_risk, df_audit, err = fetch_dashboard_data(date_str, selected_h3, selected_cell)

if err:
    st.error(err)
    st.stop()
if df_spatial is None or df_spatial.empty:
    st.warning(f"Phân vùng ngày {date_str} không có dữ liệu để vẽ.")
    st.stop()

if alert_mode:
    df_spatial = df_spatial[df_spatial['avg_rsrp'] < -100]

csv = df_spatial.to_csv(index=False).encode('utf-8')
st.sidebar.download_button(label="⬇️ Tải Data CSV hiện tại", data=csv, file_name=f'mdt_coverage_{date_str}.csv', mime='text/csv')

# =========================================================================
# KPI PANEL MỞ BÀI
# =========================================================================
st.header(f"📍 Báo cáo Vùng Phủ MDT - {date_str} ({chart_title_suffix})")

c1, c2, c3, c4 = st.columns(4)
total_users = df_spatial['user_density'].sum()
weak_count = df_spatial[df_spatial['avg_rsrp'] < -110].shape[0]
avg_rsrp_overall = df_spatial['avg_rsrp'].mean()

with c1: st.metric("Tổng thiết bị (User Density)", f"{total_users:,}")
with c2: st.metric("Trạm báo động (< -110 dBm)", f"{weak_count:,}", delta=f"{weak_count} trạm yếu", delta_color="inverse")
with c3: st.metric("RSRP Trung bình hệ thống", f"{avg_rsrp_overall:.1f} dBm")
with c4: st.metric("Băng thông trung bình", "20 MHz")

if weak_count > 500:
    st.error(f"⚠️ CẢNH BÁO MẠNG LƯỚI: Đang có {weak_count} điểm đo có tín hiệu RSRP rơi vào ngưỡng rất kém. Đề nghị tối ưu góc ngẩng (tilt).")

if not df_risk.empty:
    overload_risk_cells = df_risk['overload_risk_cells'].iloc[0]
    if overload_risk_cells > 0:
        st.warning(f"🤖 **AI SON PROACTIVE ALERT**: Phát hiện **{overload_risk_cells}** trạm có xác suất cao chạm ngưỡng nghẽn vật lý trong thời gian tới (Upper Bound > Max Capacity). Đề nghị chủ động Load Balancing.")

# =========================================================================
# CHUẨN BỊ DỮ LIỆU JSON TỪ PANDAS
# =========================================================================
df_history['record_time_dt'] = pd.to_datetime(df_history['record_time_dt'])
df_history['is_forecast'] = False
df_history['upper_density'] = df_history['total_density'] 
# Khởi tạo cận rủi ro lịch sử (trùng với thực tế để biểu đồ gọn gàng)
df_history['lower_rsrp'] = df_history['mean_rsrp']
df_history['upper_rsrp'] = df_history['mean_rsrp']

if not df_forecast.empty:
    df_forecast['record_time_dt'] = pd.to_datetime(df_forecast['record_time_dt'])
    last_time = df_history['record_time_dt'].max()
    df_forecast = df_forecast[df_forecast['record_time_dt'] > last_time].head(3)
    
    if not df_forecast.empty:
        df_forecast['poor_quality_cells'] = df_history['poor_quality_cells'].iloc[-1]
        df_forecast['is_forecast'] = True
        df_forecast = df_forecast.rename(columns={'forecast_density': 'total_density', 'forecast_upper': 'upper_density'})
        combined_trend = pd.concat([df_history, df_forecast], ignore_index=True)
    else:
        combined_trend = df_history.copy()
else:
    combined_trend = df_history.copy()

combined_trend['hour_label'] = combined_trend['record_time_dt'].dt.strftime('%H:00')

js_labels = json.dumps(combined_trend['hour_label'].tolist())
js_density = json.dumps(combined_trend['total_density'].tolist())
js_upper_density = json.dumps(combined_trend['upper_density'].round(0).tolist())
js_rsrp = json.dumps(combined_trend['mean_rsrp'].round(2).tolist())
js_rsrp_lower = json.dumps(combined_trend['lower_rsrp'].round(2).tolist())
js_rsrp_upper = json.dumps(combined_trend['upper_rsrp'].round(2).tolist())
js_poor_cells = json.dumps(combined_trend['poor_quality_cells'].tolist())
js_is_forecast = json.dumps(combined_trend['is_forecast'].tolist())

dow_map = {1: "Thứ 2", 2: "Thứ 3", 3: "Thứ 4", 4: "Thứ 5", 5: "Thứ 6", 6: "Thứ 7", 7: "Chủ Nhật"}
heatmap_dict = {f"{h}_{d}": 0.0 for d in range(1, 8) for h in range(24)}
max_density = float(df_hm['avg_density'].max()) if not df_hm.empty and df_hm['avg_density'].max() > 0 else 1

if not df_hm.empty:
    for _, row in df_hm.iterrows():
        if int(row['dow_num']) in dow_map: 
            heatmap_dict[f"{int(row['hour'])}_{int(row['dow_num'])}"] = float(row['avg_density'])
            
heatmap_list = [{"x": f"{h:02d}:00", "y": dow_map[d], "v": heatmap_dict[f"{h}_{d}"]} for d in range(1, 8) for h in range(24)]
js_heatmap_data = json.dumps(heatmap_list)

# =========================================================================
# VẼ BẢN ĐỒ VÀ CHART.JS
# =========================================================================
esri_style = {
    "version": 8,
    "sources": {
        "esri-satellite": {"type": "raster", "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"], "tileSize": 256},
        "esri-labels": {"type": "raster", "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}"], "tileSize": 256}
    },
    "layers": [
        {"id": "satellite-layer", "type": "raster", "source": "esri-satellite", "minzoom": 0, "maxzoom": 22},
        {"id": "labels-layer", "type": "raster", "source": "esri-labels", "minzoom": 0, "maxzoom": 22}
    ]
}
b64_style = base64.b64encode(json.dumps(esri_style).encode("utf-8")).decode("utf-8")
style_data_uri = f"data:application/json;base64,{b64_style}"

map_config = {
    "version": "v1",
    "config": {
        "visState": {
            "filters": [{"dataId": ["MDT_Coverage_Live"], "id": "time-filter", "name": ["record_time"], "type": "timeRange", "enlarged": True }],
            "layers": [
                {
                    "id": "h3-hexagon-layer", "type": "hexagonId",
                    "config": {
                        "dataId": "MDT_Coverage_Live", "columns": {"hex_id": "h3_index"},
                        "colorField": {"name": "avg_rsrp", "type": "real"}, "colorScale": "quantize",
                        "sizeField": {"name": "user_density", "type": "integer"}, "sizeScale": "linear",
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.8, "coverage": 0.95, "enable3d": True, "elevationScale": 0, "sizeRange": [0, 500],
                            "colorRange": {"name": "ColorBrewer RdYlGn-6", "type": "diverging", "category": "ColorBrewer", "colors": ["#d73027", "#fc8d59", "#fee08b", "#d9ef8b", "#91cf60", "#1a9850"]}
                        }
                    }
                },
                {
                    "id": "arc-signal-layer", "type": "arc",
                    "config": {
                        "dataId": "MDT_Coverage_Live", "color": [248, 149, 112],
                        "columns": {"lat0": "cell_lat", "lng0": "cell_lon", "lat1": "h3_center_lat", "lng1": "h3_center_lon"},
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.8, "thickness": 2, "targetColor": [255, 203, 153],
                            "colorRange": {"name": "Global Warming", "type": "sequential", "category": "Uber", "colors": ["#5A1846", "#900C3F", "#C70039", "#E3611C", "#F1920E", "#FFC300"]}
                        }
                    }
                }
            ]
        },
        "mapState": {"latitude": 20.9740, "longitude": 105.7745, "zoom": 10, "pitch": 45, "bearing": 0},
        "mapStyle": {"styleType": "esri_satellite", "mapStyles": {"esri_satellite": {"id": "esri_satellite", "label": "ESRI Satellite", "url": style_data_uri, "custom": True}}},
        "interactionConfig": {
            "tooltip": {
                "fieldsToShow": {
                    "MDT_Coverage_Live": [
                        {"name": "h3_index", "format": None}, {"name": "cell_id", "format": None}, {"name": "Health Status", "format": None},
                        {"name": "avg_rsrp", "format": "0.01f"}, {"name": "p10_rsrp", "format": "0.01f"}, {"name": "user_density", "format": None}, {"name": "avg_distance_km", "format": "0.01f"}
                    ]
                },
                "compareMode": False, "compareType": "absolute", "enabled": True
            }
        }
    }
}

m = KeplerGl(height=900, config=map_config)
df_for_map = df_spatial.drop(columns=['record_time_dt'], errors='ignore')
m.add_data(data=df_for_map, name="MDT_Coverage_Live")
if not df_audit.empty: m.add_data(data=df_audit, name="Quality_Audit_Report")

temp_html_path = "/tmp/kepler_temp.html"
m.save_to_html(file_name=temp_html_path, read_only=False)
with open(temp_html_path, "r", encoding="utf-8") as f: html_content = f.read()

dashboard_injection = f"""
<style>
    html, body {{ width: 100% !important; height: 100vh !important; margin: 0 !important; overflow: hidden !important; }}
    #insight-panel {{ position: absolute; top: 15px; right: 15px; width: 420px; background: rgba(18, 20, 22, 0.9); backdrop-filter: blur(8px); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; color: #e2e8f0; z-index: 9999; box-shadow: 0 10px 25px rgba(0,0,0,0.5); font-family: 'Segoe UI', sans-serif; }}
    .panel-header {{ display: flex; justify-content: space-between; align-items: center; padding: 15px 20px; border-bottom: 1px solid rgba(255,255,255,0.1); }}
    .insight-title {{ font-size: 16px; margin: 0; font-weight: bold; color: #fff; }}
    #toggle-panel-btn {{ background: none; border: none; color: #a0aec0; font-size: 18px; cursor: pointer; }}
    #panel-content {{ padding: 0 20px 20px 20px; max-height: 80vh; overflow-y: auto; }}
    .chart-container {{ margin-top: 20px; }}
    .rsrp-legend-container {{ position: absolute; bottom: 35px; left: 50%; transform: translateX(-50%); display: flex; flex-direction: row; box-shadow: 0 6px 12px rgba(0,0,0,0.4); border: 2px solid rgba(255,255,255,0.8); border-radius: 6px; overflow: hidden; z-index: 10000; }}
    .rsrp-box {{ display: flex; flex-direction: column; align-items: center; justify-content: center; width: 100px; height: 50px; font-family: 'Segoe UI', sans-serif; font-size: 12px; font-weight: bold; text-align: center; line-height: 1.2; }}
    .rsrp-box span:nth-child(2) {{ font-size: 10px; font-weight: normal; margin-top: 2px; }}
    .c-1 {{ background-color: #d73027; color: #fff; text-shadow: 0px 0px 3px rgba(0,0,0,0.8); }} .c-2 {{ background-color: #fc8d59; color: #000; }} .c-3 {{ background-color: #fee08b; color: #000; }} .c-4 {{ background-color: #d9ef8b; color: #000; }} .c-5 {{ background-color: #91cf60; color: #000; }} .c-6 {{ background-color: #1a9850; color: #fff; text-shadow: 0px 0px 3px rgba(0,0,0,0.8); }}
</style>
<div class="rsrp-legend-container">
    <div class="rsrp-box c-1"><span>Rất Kém</span><span>&lt; -110 dBm</span></div><div class="rsrp-box c-2"><span>Kém</span><span>-110 ÷ -100</span></div><div class="rsrp-box c-3"><span>TB-Yếu</span><span>-100 ÷ -90</span></div><div class="rsrp-box c-4"><span>Trung Bình</span><span>-90 ÷ -80</span></div><div class="rsrp-box c-5"><span>Khá</span><span>-80 ÷ -70</span></div><div class="rsrp-box c-6"><span>Tốt</span><span>&gt; -70 dBm</span></div>
</div>
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
    const pointStyles = isForecast.map(f => f ? 'triangle' : 'circle'); const segmentStyles = ctx => ctx.p1DataIndex >= labels.length - 3 ? [5, 5] : undefined;
    const commonOptions = {{ responsive: true, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ ticks: {{ color: '#a0aec0' }} }}, y: {{ ticks: {{ color: '#a0aec0' }} }} }} }};

    new Chart(document.getElementById('chartHeatmap'), {{ type: 'matrix', data: {{ datasets: [{{ label: 'Tải hệ thống', data: {js_heatmap_data}, backgroundColor: ctx => {{ if (!ctx.dataset.data[ctx.dataIndex]) return 'transparent'; const v = ctx.dataset.data[ctx.dataIndex].v; if (v === 0) return 'rgba(255, 255, 255, 0.05)'; const pct = v / {max_density}; if (pct >= 0.8) return '#081d58'; if (pct >= 0.6) return '#225ea8'; if (pct >= 0.4) return '#41b6c4'; if (pct >= 0.2) return '#c7e9b4'; return '#ffffcc'; }}, borderColor: 'rgba(255,255,255,0.05)', borderWidth: 1, width: c => (c.chartArea ? c.chartArea.width : 300) / 24 - 1, height: c => (c.chartArea ? c.chartArea.height : 100) / 7 - 1 }}] }}, options: {{ maintainAspectRatio: false, plugins: {{ title: {{ display: true, text: 'Ma trận tải tuần hoàn ({chart_title_suffix})', color: '#fff' }} }}, scales: {{ x: {{ type: 'category', labels: {json.dumps([f"{i:02d}:00" for i in range(24)])}, ticks: {{ color: '#a0aec0', maxTicksLimit: 8 }} }}, y: {{ type: 'category', labels: {json.dumps(["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"])}, ticks: {{ color: '#a0aec0' }} }} }} }} }});
    new Chart(document.getElementById('chartPoor'), {{ type: 'bar', data: {{ labels: labels, datasets: [{{ label: 'Khu vực kém', data: {js_poor_cells}, backgroundColor: isForecast.map(f => f ? 'rgba(245, 101, 101, 0.4)' : '#f56565') }}] }}, options: {{ ...commonOptions, plugins: {{ title: {{ display: true, text: 'Cảnh báo vùng chất lượng kém ({chart_title_suffix})', color: '#fff' }} }} }} }});
    
    new Chart(document.getElementById('chartDensity'), {{ 
        type: 'line', 
        data: {{ 
            labels: labels, 
            datasets: [
                {{ label: 'Rủi ro bùng phát tối đa (Upper Bound)', data: {js_upper_density}, borderColor: 'rgba(245, 101, 101, 0.8)', backgroundColor: 'transparent', pointRadius: 0, borderDash: [3, 3], borderWidth: 2, tension: 0.3 }}, 
                {{ label: 'User Density', data: {js_density}, borderColor: '#38b2ac', backgroundColor: 'rgba(56, 178, 172, 0.2)', fill: true, pointStyle: pointStyles, tension: 0.3, segment: {{ borderDash: segmentStyles }} }}
            ] 
        }}, 
        options: {{ ...commonOptions, plugins: {{ title: {{ display: true, text: 'Xu hướng User Density & Rủi ro 3h ({chart_title_suffix})', color: '#fff' }} }} }} 
    }});

    new Chart(document.getElementById('chartRsrp'), {{ 
        type: 'line', 
        data: {{ 
            labels: labels, 
            datasets: [
                {{ label: 'Biên độ nhiễu tối đa (Upper RSRP)', data: {js_rsrp_upper}, borderColor: 'rgba(236, 201, 75, 0.4)', backgroundColor: 'transparent', pointRadius: 0, borderDash: [3, 3], borderWidth: 1, tension: 0.3 }},
                {{ label: 'Rủi ro suy hao sóng (Lower RSRP)', data: {js_rsrp_lower}, borderColor: 'rgba(236, 201, 75, 0.4)', backgroundColor: 'rgba(236, 201, 75, 0.1)', fill: '-1', pointRadius: 0, borderDash: [3, 3], borderWidth: 1, tension: 0.3 }},
                {{ label: 'Avg RSRP', data: {js_rsrp}, borderColor: '#ecc94b', backgroundColor: 'transparent', pointStyle: pointStyles, tension: 0.3, segment: {{ borderDash: segmentStyles }} }}
            ] 
        }}, 
        options: {{ ...commonOptions, plugins: {{ title: {{ display: true, text: 'Biến động & Dải rủi ro Avg RSRP ({chart_title_suffix})', color: '#fff' }} }} }} 
    }});
</script>
"""
parts = html_content.rsplit("</body>", 1)
components.html(f"{parts[0]}{dashboard_injection}</body>{parts[1]}", height=850, scrolling=True)
if os.path.exists(temp_html_path): os.remove(temp_html_path)