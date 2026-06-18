import argparse
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(f"Fact_Builder_{args.date}_{args.hour}")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

    # UDF sinh H3
    @F.udf(returnType=StringType())
    def get_h3_index(lat, lon):
        try:
            if lat is None or lon is None: 
                return None
            
            import h3
            f_lat = float(lat)
            f_lon = float(lon)
            
            # Kiểm tra nếu là H3 phiên bản v4 (Có latlng_to_cell)
            if hasattr(h3, 'latlng_to_cell'):
                return h3.latlng_to_cell(f_lat, f_lon, 9)
            # Nếu là H3 phiên bản v3 cũ (Có geo_to_h3)
            elif hasattr(h3, 'geo_to_h3'):
                return h3.geo_to_h3(f_lat, f_lon, 9)
            else:
                return "ERROR: Unknown H3 library version"
                
        except Exception as e: 
            return f"ERROR: {str(e)}"

    clean_input = "s3a://silver/mdt_clean/"
    dim_cell_path = "s3a://gold/dim_cell/"
    fact_output = "s3a://gold/fact_mdt_hourly/"

    try:
        # 1. Đọc MDT sạch & Gắn H3
        mdt_df = spark.read.format("delta").load(clean_input) \
                      .filter((F.col("date") == args.date) & (F.col("hour") == args.hour)) \
                      .withColumnRenamed("cell_code", "cell_id") \
                      .withColumn("cell_id", F.upper(F.regexp_replace(F.col("cell_id"), r"\s+", ""))) \
                      .withColumn("h3_index", get_h3_index(F.col("lat"), F.col("lng")))
        
        print("=== DEBUG 5 DÒNG KẾT QUẢ SINH H3 ĐẦU TIÊN ===")
        mdt_df.select("lat", "lng", "h3_index").show(5, truncate=False)              
        if mdt_df.count() == 0: 
            print("Không có dữ liệu, dừng an toàn.")
            sys.exit(0)

        # 2. Đọc Dim Cell VÀ LÀM SẠCH KHÓA (Rất quan trọng)
        dim_cell = spark.read.format("delta").load(dim_cell_path) \
                        .withColumn("cell_code", F.upper(F.regexp_replace(F.col("cell_code"), r"\s+", "")))

        # 3. POINT-IN-TIME JOIN (Lấy đúng cell_sk theo ngày đo)
        joined_df = mdt_df.join(
            broadcast(dim_cell),
            mdt_df["cell_id"] == dim_cell["cell_code"],
            how="left"
        )

        # 4. TÍNH KHOẢNG CÁCH
        R = 6371.0
        df_math = joined_df.withColumn(
            "distance_to_cell_km",
            F.acos(
                F.sin(F.radians(F.col("lat"))) * F.sin(F.radians(F.col("station_lat"))) + 
                F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("station_lat"))) * F.cos(F.radians(F.col("station_lng") - F.col("lng")))
            ) * R
        )

        # 5. AGGREGATE THÀNH FACT CHUẨN (Chỉ lấy FK và KPI)
        rsrp_col = "p_cell_rsrp"
        fact_df = df_math.groupBy(
            "h3_index", "cell_id", "date", "hour" 
        ).agg(
            F.count("*").alias("user_density_count"),
            F.round(F.avg(rsrp_col), 2).alias("avg_rsrp"),
            F.expr(f"percentile_approx({rsrp_col}, 0.1)").alias("p10_rsrp"),
            F.round(F.avg("distance_to_cell_km"), 2).alias("avg_distance_km"),
            F.round(F.avg(F.when(F.col(rsrp_col) < -110, 1).otherwise(0)), 4).alias("weak_signal_ratio")
        )

        # 6. Ghi xuống Gold
        print(f" Đang ghi Fact table cho ca {args.date} {args.hour}:00...")
        fact_df.write.format("delta").mode("overwrite") \
            .option("replaceWhere", f"date = '{args.date}' AND hour = '{args.hour}'") \
            .option("mergeSchema", "true") \
            .partitionBy("date", "hour") \
            .save(fact_output)
            
        print(f" Tạo Fact Model hoàn tất!")
        
    except Exception as e:
        print(f" Lỗi luồng Fact: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()