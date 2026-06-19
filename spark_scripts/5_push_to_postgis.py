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

        # Danh sách cấu trúc Star Schema
        tables = {
            "fact_mdt_hourly": "s3://gold/fact_mdt_hourly/",  
            "dim_cell": "s3://gold/dim_cell/",               
            "dim_h3": "s3://gold/dim_h3/",               
            "dim_date": "s3://gold/dim_date/",               
            "quality_report": "s3://silver/quality_report/",
            "fact_forecast_hourly": "s3://gold/fact_forecast_hourly/"  
        }

        for table_name, path in tables.items():
            print(f"🔄 Đang cập nhật metadata cho bảng: {table_name}...")
            
            # TUYỆT CHIÊU XỬ LÝ SCHEMA MISMATCH:
            # Drop table để xóa cache cũ (yên tâm KHÔNG mất data ở MinIO vì đây là External Table)
            try:
                cur.execute(f"DROP TABLE IF EXISTS lakehouse.default.{table_name}")
                cur.fetchall()
            except Exception:
                pass

            # Đăng ký lại bảng, Trino sẽ tự động quét file Delta log và nhận diện các cột mới thêm
            print(f"  -> Đang register lại {table_name} với schema mới nhất...")
            cur.execute(f"CALL lakehouse.system.register_table('default', '{table_name}', '{path}')")
            cur.fetchall()

            # Tối ưu hóa hiệu năng truy vấn (Z-Ordering / Vacuum)
            try:
                cur.execute(f"ALTER TABLE lakehouse.default.{table_name} EXECUTE OPTIMIZE")
                cur.fetchall()
                cur.execute(f"ALTER TABLE lakehouse.default.{table_name} EXECUTE VACUUM (retention => '7d')")
                cur.fetchall()
            except Exception:
                pass

        print("✅ Xuất bản cấu trúc & Đồng bộ Schema Lakehouse HOÀN TẤT!")

    except Exception as e:
        print(f"❌ Lỗi khi tương tác với Trino Metastore: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()