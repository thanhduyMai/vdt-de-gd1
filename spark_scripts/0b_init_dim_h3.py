from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def generate_h3_for_strip(lat):
    """
    Hàm này sẽ chạy trực tiếp trên các Worker dưới dạng phân tán.
    Mỗi worker xử lý 1 dải vĩ độ và sinh ra h3_index kèm tọa độ cùng một lúc.
    """
    import h3  # Import bên trong để worker nào cũng có thể gọi độc lập
    
    strip_poly = {
        "type": "Polygon",
        "coordinates": [[[102.1, lat], [109.5, lat], [109.5, lat+1], [102.1, lat+1], [102.1, lat]]]
    }
    
    # Sinh danh sách các ô H3 cho dải vĩ độ hiện tại
    indices = h3.polyfill(strip_poly, 9, geo_json_conformant=True)
    
    # Trả về dữ liệu dạng generator giúp tiết kiệm RAM tối đa cho worker
    for h3_index in indices:
        lat_center, lon_center = h3.h3_to_geo(h3_index)
        yield (h3_index, float(lat_center), float(lon_center))

def main():
    # 0. Khởi tạo SparkSession
    spark = SparkSession.builder.appName("Init_Dim_H3") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("🚀 Đang gửi cấu hình dải vĩ độ xuống các Worker...", flush=True)
    
    # Tạo danh sách vĩ độ từ 8 đến 23 (Chỉ có 16 phần tử trên Driver!)
    latitudes = list(range(8, 24))
    
    # Phân bổ thành 16 partitions, tương ứng mỗi partition xử lý một dải vĩ độ độc lập
    rdd_lat = spark.sparkContext.parallelize(latitudes, numSlices=len(latitudes))
    
    # Sử dụng flatMap để các worker đồng loạt sinh dữ liệu H3 và tọa độ center
    rdd_h3 = rdd_lat.flatMap(generate_h3_for_strip)
    
    # Định nghĩa Schema tường minh cho DataFrame kết quả
    schema = StructType([
        StructField("h3_index", StringType(), True),
        StructField("h3_center_lat", DoubleType(), True),
        StructField("h3_center_lon", DoubleType(), True)
    ])
    
    # Chuyển đổi trực tiếp thành DataFrame
    df_dim = rdd_h3.toDF(schema)

    # 3. Ghi dữ liệu vào Delta Lake ở vùng Gold
    output_path = "s3a://gold/dim_h3/"
    print("💾 Đang ghi dữ liệu phân tán vào Delta Lake...", flush=True)
    
    # Gộp về 50 file và ghi trực tiếp (save() là hàm chặn nên hoàn toàn an tâm)
    df_dim.repartition(50).write.format("delta").mode("overwrite").save(output_path)
    
    print("🎉 Đã ghi xong dữ liệu vào Delta Lake một cách an toàn và tối ưu!", flush=True)
    spark.stop()
    
if __name__ == "__main__":
    main()