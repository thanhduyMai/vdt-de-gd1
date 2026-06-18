import argparse
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=True, choices=['mdt', 'cell'], help="Loại dữ liệu cần ingest")
    parser.add_argument("--date", required=False, help="Dùng cho MDT (YYYY-MM-DD)")
    parser.add_argument("--hour", required=False, help="Dùng cho MDT (HH)")
    parser.add_argument("--input_path", required=False, help="Truyền động từ DAG")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(f"Universal_Ingestor_{args.type}")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") 
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    if args.type == 'cell':
        input_path = "s3a://landingzone/cell_conf_5g_nsa_hadong.csv"
        output_path = "s3a://bronze/cell_raw/"
        print(" Đang Ingest dữ liệu Cell...")
        
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").save(output_path)
        print(" Ingest Cell hoàn tất!")

    elif args.type == 'mdt':
        if not args.date or not args.hour:
            print(" Lỗi: Ingest MDT bắt buộc phải có --date và --hour")
            sys.exit(1)
            
        input_path = args.input_path if args.input_path else f"s3a://landingzone/mdt_{args.date.replace('-','')}{args.hour}.csv"
        output_path = "s3a://bronze/mdt_raw/"
        print(f" Đang Ingest dữ liệu MDT từ {input_path} ca {args.date} {args.hour}:00...")
        
        raw_df = spark.read.csv(input_path, header=True)
        if len(raw_df.columns) == 0:
            print(" File trống, luồng dừng an toàn.")
            sys.exit(0)

        # Xử lý partition chuẩn theo CSV mới
        if "time_ms" in raw_df.columns:
            df_parsed = raw_df.withColumn("time_ms", F.col("time_ms").cast("long")) \
                              .withColumn("actual_ts", F.from_unixtime(F.col("time_ms") / 1000).cast("timestamp"))
            
            df_with_partitions = df_parsed.withColumn("date", F.date_format("actual_ts", "yyyy-MM-dd")) \
                                          .withColumn("hour", F.date_format("actual_ts", "HH")) \
                                          .withColumn("date_hour", F.concat_ws("-", F.col("date"), F.col("hour"))) \
                                          .withColumn("batch_date", F.lit(args.date)) \
                                          .withColumn("batch_hour", F.lit(args.hour)) \
                                          .drop("actual_ts")
        else:
            df_with_partitions = raw_df.withColumn("date", F.lit(args.date)) \
                                       .withColumn("hour", F.lit(args.hour)) \
                                       .withColumn("date_hour", F.concat_ws("-", F.lit(args.date), F.lit(args.hour))) \
                                       .withColumn("batch_date", F.lit(args.date)) \
                                       .withColumn("batch_hour", F.lit(args.hour))

        replace_cond = f"batch_date = '{args.date}' AND batch_hour = '{args.hour}'"
        df_with_partitions.write.format("delta").mode("overwrite") \
            .option("replaceWhere", replace_cond) \
            .option("mergeSchema", "true") \
            .partitionBy("batch_date", "batch_hour").save(output_path)
            
        print(" Ingest MDT hoàn tất!")

if __name__ == "__main__":
    main()