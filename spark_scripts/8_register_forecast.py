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
            "fact_forecast_hourly": "s3a://gold/fact_forecast_hourly/",
            "fact_model_evaluation": "s3a://gold/fact_model_evaluation/"
        }

        for table_name, path in tables.items():
            print(f"🔄 Đang xử lý đồng bộ Metadata cho bảng: {table_name}...")
            
            # 1. XOÁ DANH BẠ CŨ TRÊN TRINO (Để ép Trino nhận diện schema mới, file trên MinIO vẫn an toàn)
            cur.execute(f"DROP TABLE IF EXISTS lakehouse.default.{table_name}")
            cur.fetchall()
            
            # 2. ĐĂNG KÝ LẠI BẢNG (Có Try-Except chống sập nếu thư mục rỗng)
            print(f"  -> Đang đăng ký lại bảng với cấu trúc mới nhất...")
            query_register = f"""
                CALL lakehouse.system.register_table(
                    schema_name => 'default', 
                    table_name => '{table_name}', 
                    table_location => '{path}'
                )
            """
            
            try:
                cur.execute(query_register)
                cur.fetchall()
                print(f"  ✅ Đã đồng bộ thành công bảng {table_name}!")
            except Exception as reg_err:
                # Nếu bảng chưa hề có data (như bảng Evaluation chạy lần đầu), Trino sẽ ném lỗi "No transaction log"
                if "No transaction log found" in str(reg_err):
                    print(f"  ⚠️ Bỏ qua {table_name}: Thư mục trống hoặc chưa có data. Hệ thống sẽ tự đăng ký vào các ca chạy sau.")
                else:
                    # Nếu là lỗi lạ khác thì mới văng lỗi ra để Airflow bắt
                    raise reg_err

        print("✅ Xuất bản cấu trúc & Đồng bộ Schema Lakehouse HOÀN TẤT MỸ MÃN!")

    except Exception as e:
        print(f"❌ Lỗi khi tương tác với Trino Metastore: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()