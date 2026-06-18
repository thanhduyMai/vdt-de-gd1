from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType
import h3

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

    # 1. Hàm tạo dải vĩ độ
    def generate_h3_for_strips():
        all_h3 = []
        for lat in range(8, 24):
            print(f"DEBUG: Đang xử lý dải lat {lat}...", flush=True)
            strip_poly = {
                "type": "Polygon",
                "coordinates": [[[102.1, lat], [109.5, lat], [109.5, lat+1], [102.1, lat+1], [102.1, lat]]]
            }
            indices = list(h3.polyfill(strip_poly, 9, geo_json_conformant=True))
            all_h3.extend(indices)
        return all_h3

    print("🚀 Đang tính toán H3 theo dải vĩ độ...", flush=True)
    h3_indices = generate_h3_for_strips()
    
    if h3_indices is None or len(h3_indices) == 0:
        print("LỖI: H3_indices bị rỗng!", flush=True)
        return

    print(f"✅ Đã có {len(h3_indices)} ô lục giác. Đang đẩy vào Spark...", flush=True)

    # 2. Tạo DataFrame
    rdd = spark.sparkContext.parallelize(h3_indices, numSlices=200)
    df = rdd.map(lambda h: (h,)).toDF(["h3_index"])

    # 3. UDF lấy tọa độ
    @F.udf(returnType=StringType())
    def get_lat(h3_index):
        lat, lon = h3.h3_to_geo(h3_index)
        return str(lat)

    @F.udf(returnType=StringType())
    def get_lon(h3_index):
        lat, lon = h3.h3_to_geo(h3_index)
        return str(lon)

    df_dim = df.withColumn("h3_center_lat", get_lat(F.col("h3_index")).cast(DoubleType())) \
               .withColumn("h3_center_lon", get_lon(F.col("h3_index")).cast(DoubleType()))

    # 4. Ghi vào Gold
    output_path = "s3a://gold/dim_h3/"
    df_dim.repartition(50).write.format("delta").mode("overwrite").save(output_path)
    
    # Dòng này cực quan trọng: Ép Spark không được đóng cho đến khi ghi xong
    count = df_dim.count() 
    print(f"🎉 Đã ghi xong {count} dòng dữ liệu vào Delta!", flush=True)
    spark.stop()
    
if __name__ == "__main__":
    main()