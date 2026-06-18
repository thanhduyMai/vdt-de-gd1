from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = SparkSession.builder.appName("Init_Dim_Date") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Sinh danh sách ngày từ 2020-01-01 đến 2030-12-31
    df = spark.sql("SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) as date")
    
    # Thêm các thuộc tính của ngày
    df_dim = df.select(
        F.col("date").cast("string").alias("date_key"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name")
    )
    
    output_path = "s3a://gold/dim_date/"
    df_dim.write.format("delta").mode("overwrite").save(output_path)
    print("✅ Đã khởi tạo bảng dim_date (2020-2030) thành công!")

if __name__ == "__main__":
    main()