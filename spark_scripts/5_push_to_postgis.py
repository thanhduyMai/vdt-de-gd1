import argparse
import sys
import trino

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    print(f"🔗 Đang đồng bộ danh bạ Lakehouse cho ca {args.date} {args.hour}:00...")

    try:
        conn = trino.dbapi.connect(
            host="mdt_trino", port=8080, user="admin", catalog="lakehouse", schema="default"
        )
        cur = conn.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS lakehouse.default WITH (location = 's3://gold/')")
        cur.fetchall()

        # Danh sách cấu trúc Star Schema mới
        tables = {
            "fact_mdt_hourly": "s3://gold/fact_mdt_hourly/",  
            "dim_cell": "s3://gold/dim_cell/",               
            "dim_h3": "s3://gold/dim_h3/",               
            "dim_date": "s3://gold/dim_date/",               
            "quality_report": "s3://silver/quality_report/",
            "fact_forecast_hourly": "s3://gold/fact_forecast_hourly/"  
        }

        for table_name, path in tables.items():
            print(f" Đang kiểm tra metadata cho bảng: {table_name}...")
            
            cur.execute(f"SHOW TABLES IN lakehouse.default LIKE '{table_name}'")
            table_exists = cur.fetchone()

            if not table_exists:
                print(f"Bảng {table_name} chưa tồn tại. Đang register...")
                cur.execute(f"CALL lakehouse.system.register_table('default', '{table_name}', '{path}')")
                cur.fetchall()
            else:
                print(f"Bảng {table_name} đã tồn tại. Sẽ tự động sync metadata.")

            # Dọn rác
            try:
                cur.execute(f"ALTER TABLE lakehouse.default.{table_name} EXECUTE OPTIMIZE")
                cur.fetchall()
                cur.execute(f"ALTER TABLE lakehouse.default.{table_name} EXECUTE VACUUM (retention => '7d')")
                cur.fetchall()
            except Exception:
                pass

        print("✅ Xuất bản cấu trúc & Dọn rác Lakehouse HOÀN TẤT!")

    except Exception as e:
        print(f"❌ Lỗi khi tương tác với Trino Metastore: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()