import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable  # Import thêm thư viện DeltaTable để làm MERGE

def main():
    spark = SparkSession.builder \
        .appName("Update_Dim_Cell_SCD1") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("🚀 Đang kéo file cấu hình trạm từ MinIO Landing Zone...")

    input_csv = "s3a://landingzone/cell_info.csv" 
    output_delta = "s3a://gold/dim_cell/"

    try:
        # 1. Đọc và chuẩn hóa dữ liệu MỚI từ CSV
        df_cell = spark.read.csv(input_csv, header=True, inferSchema=True)

        df_clean = df_cell.select(
            F.col("cell_code").cast("string"),
            F.col("x").cast("double").alias("station_lat"), 
            F.col("y").cast("double").alias("station_lng"),  
            F.col("station_code").cast("string").alias("sector_code"),
            F.col("azimuth").cast("double"),
            F.col("province_name").cast("string"),
            F.col("district_name").cast("string")
        )
        
        # Khử trùng lặp file nguồn để tránh lỗi Merge
        df_new = df_clean.dropDuplicates(["cell_code"])

        # ====================================================================
        # 2. THỰC HIỆN SCD TYPE 1 (UPSERT) BẰNG DELTA LAKE MERGE
        # ====================================================================
        if DeltaTable.isDeltaTable(spark, output_delta):
            print(" Đã tìm thấy bảng dim_cell cũ. Tiến hành MERGE (Upsert) SCD Type 1...")
            deltaTable = DeltaTable.forPath(spark, output_delta)
            
            deltaTable.alias("old").merge(
                df_new.alias("new"),
                "old.cell_code = new.cell_code" # Điều kiện khớp Khóa (Key)
            ).whenMatchedUpdateAll( # Nếu Khóa đã tồn tại -> Cập nhật toàn bộ các cột khác
            ).whenNotMatchedInsertAll( # Nếu Khóa chưa từng có -> Thêm dòng mới
            ).execute()
            
            print(" Cập nhật SCD Type 1 hoàn tất!")
        else:
            # Nếu chạy lần đầu tiên, chưa có bảng
            print(" Chưa có bảng dim_cell. Tiến hành tạo mới (Initial Load)...")
            df_new.write.format("delta").mode("overwrite").save(output_delta)

    except Exception as e:
        print(f" Lỗi nạp/cập nhật dim_cell: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
