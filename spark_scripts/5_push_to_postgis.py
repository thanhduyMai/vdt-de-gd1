import argparse
import sys
import trino

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    print(f"🔗 Đang kiểm tra danh bạ Lakehouse cho ca {args.date} {args.hour}:00...")

    try:
        conn = trino.dbapi.connect(
            host="mdt_trino", port=8080, user="admin", catalog="lakehouse", schema="default"
        )
        cur = conn.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS lakehouse.default WITH (location = 's3a://gold/')")
        cur.fetchall()

        tables = {
            "fact_mdt_hourly": "s3a://gold/fact_mdt_hourly/",  
            "dim_cell": "s3a://gold/dim_cell/",              
            "dim_h3": "s3a://gold/dim_h3/",              
            "dim_date": "s3a://gold/dim_date/",              
            "quality_report": "s3a://silver/quality_report/",
            #"fact_forecast_hourly": "s3a://gold/fact_forecast_hourly/"  
        }

        for table_name, path in tables.items():
            print(f"🔄 Kiểm tra metadata cho bảng: {table_name}...")
            
            # 1. KIỂM TRA BẢNG ĐÃ TỒN TẠI CHƯA
            cur.execute(f"SHOW TABLES IN lakehouse.default LIKE '{table_name}'")
            exists = cur.fetchone()
            
            if exists:
                print(f"  -> Bảng '{table_name}' ĐÃ TỒN TẠI. Bỏ qua bước Register (Delta sẽ tự auto-sync data mới).")
            else:
                print(f"  -> Bảng chưa có. Đang register LẦN ĐẦU TIÊN cho {table_name}...")
                query_register = f"""
                    CALL lakehouse.system.register_table(
                        schema_name => 'default', 
                        table_name => '{table_name}', 
                        table_location => '{path}'
                    )
                """
                cur.execute(query_register)
                cur.fetchall()
                print(f"  ✅ Đã register thành công {table_name}!")

            # 2. CHẠY TỐI ƯU HÓA (Chạy ngầm, không tốn RAM Trino vì MinIO/Spark chịu tải)
            try:
                # cur.execute(f"ALTER TABLE lakehouse.default.{table_name} EXECUTE OPTIMIZE")
                # cur.fetchall()
                pass # Tạm thời tắt Optimize qua Trino để tránh crash. Khuyên bác dùng Spark để chạy Optimize sẽ tốt hơn.
            except Exception:
                pass

        print("✅ Xuất bản cấu trúc & Đồng bộ Schema Lakehouse HOÀN TẤT!")

    except Exception as e:
        print(f"❌ Lỗi khi tương tác với Trino Metastore: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()