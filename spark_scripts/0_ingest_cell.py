import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("Init_Dim_Cell_From_LandingZone") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("🚀 Đang kéo file cấu hình trạm từ MinIO Landing Zone...")

    # 2. Định nghĩa đường dẫn
    input_csv = "s3a://landingzone/cell_info.csv" 
    output_delta = "s3a://gold/dim_cell/"

    try:
        # 3. Đọc CSV với header
        df_cell = spark.read.csv(input_csv, header=True, inferSchema=True)

        # 4. CHUẨN HÓA CẤU TRÚC MỚI
        df_clean = df_cell.select(
            F.col("cell_code").cast("string"),
            F.col("x").cast("double").alias("station_lat"), # Đổi x thành lat
            F.col("y").cast("double").alias("station_lng"), # Đổi y thành lng
            F.lit(20.0).cast("double").alias("bandwidth"),  # Tự động điền 20MHz vì file thiếu
            F.col("station_code").cast("string").alias("sector_code"),
            F.col("azimuth").cast("double"),
            F.col("province_name").cast("string"),
            F.col("district_name").cast("string")
        )
        
        # 5. Khử trùng lặp (Dedup) theo cell_code để đảm bảo tính duy nhất
        df_final = df_clean.dropDuplicates(["cell_code"])

        # 6. Ghi đè xuống Delta Lake tầng Gold
        print(" Đang ghi đè dữ liệu Trạm (dim_cell) xuống Delta Lake (Gold)...")
        df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_delta)
        
        print(f" Đã nạp thành công {df_final.count()} trạm vào Lakehouse tại s3a://gold/dim_cell/")
        
    except Exception as e:
        print(f" Lỗi nạp dim_cell: {e}")

    spark.stop()

if __name__ == "__main__":
    main()