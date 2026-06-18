import trino

try:
    print("1. Đang kết nối Trino...")
    conn = trino.dbapi.connect(host="mdt_trino", port=8080, user="admin", catalog="lakehouse", schema="default")
    cur = conn.cursor()

    # Query nguyên bản của bác
    query = """
    WITH BaseData AS ( 
        SELECT f.h3_index, f.cell_id, c.station_lat AS cell_lat, c.station_lng AS cell_lon, c.bandwidth, h.h3_center_lat, h.h3_center_lon, f.user_density_count AS user_density, f.avg_rsrp, f.avg_distance_km, f.p10_rsrp, CAST(f.hour AS INTEGER) AS hour_num, 
        date_parse(f.date || ' ' || LPAD(CAST(f.hour AS VARCHAR), 2, '0') || ':00:00', '%Y-%m-%d %H:%i:%s') AS record_time 
        FROM lakehouse.default.fact_mdt_hourly f 
        LEFT JOIN lakehouse.default.dim_cell c ON f.cell_id = UPPER(TRIM(c.cell_code)) 
        LEFT JOIN lakehouse.default.dim_h3 h ON f.h3_index = h.h3_index 
        WHERE f.date = '2025-05-04'
    ), CalculatedDiff AS ( 
        SELECT *, user_density - LAG(user_density, 1, 0) OVER (PARTITION BY cell_id, h3_index ORDER BY hour_num ASC) AS density_diff 
        FROM BaseData 
    ) 
    SELECT * FROM CalculatedDiff LIMIT 5
    """

    print("2. Đang ném lệnh SQL sang Trino (Vui lòng đợi)...")
    cur.execute(query)
    rows = cur.fetchall()
    
    print("✅ THÀNH CÔNG! Trino trả về data bình thường:")
    print(rows)

except Exception as e:
    print("\n🚨🚨🚨 ĐÂY LÀ LỖI GỐC CỦA TRINO (PANDAS ĐÃ GIẤU): 🚨🚨🚨")
    print(e)