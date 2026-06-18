import argparse
import sys
from datetime import datetime, timedelta
import pytz
import pyspark.sql.functions as F
from pyspark.sql.window import Window  
from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(f"QC_GX_{args.date}_{args.hour}")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") 
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

    vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    input_path = "s3a://bronze/mdt_raw/"
    clean_path = "s3a://silver/mdt_clean/"
    dirty_path = "s3a://silver/dirty_data/" 
    report_path = "s3a://silver/quality_report/"

    try:
        # =====================================================================
        # 🚨 ĐOẠN DEBUG: QUÉT TOÀN BỘ KHO BRONZE TRƯỚC KHI LỌC
        # =====================================================================
        full_df = spark.read.format("delta").load(input_path)
        
        print("\n" + "="*60)
        print("🕵️ MÁY QUÉT DEBUG: SỰ THẬT TRONG KHO BRONZE ĐANG CÓ GÌ?")
        print(f" -> Bạn đang yêu cầu tìm kiếm: batch_date='{args.date}' | batch_hour='{args.hour}'")
        print(" -> Dưới đây là danh sách CÁC PHÂN VÙNG ĐANG TỒN TẠI THỰC TẾ:")
        
        # In ra danh sách các cặp ngày/giờ đang có data thật
        full_df.select("batch_date", "batch_hour").distinct().show(truncate=False)
        
        total_bronze = full_df.count()
        print(f" -> TỔNG CỘNG SỐ DÒNG CỦA TOÀN BỘ KHO BRONZE: {total_bronze} dòng")
        print("="*60 + "\n")
        # =====================================================================

        # Sau khi in xong, áp dụng màng lọc như cũ
        df = full_df.filter((F.col("batch_date") == args.date) & (F.col("batch_hour") == args.hour))
        
    except Exception as e:
        print(f" Không tìm thấy bảng Delta tại {input_path} hoặc lỗi kết nối: {e}")
        sys.exit(0)

    total_raw_records = df.count()
    if total_raw_records == 0:
        print(" Phân vùng trống. Không có dữ liệu để đánh giá.")
        sys.exit(0)

    on_time_df = df
    late_count = 0
    on_time_count = total_raw_records

    # COMPOSITE KEY THEO TÊN CỘT MỚI TỪ FILE CSV
    unique_columns = ["device_code", "lat", "lng", "cell_code", "p_cell_rsrp", "p_cell_rsrq"]
    
    window_spec = Window.partitionBy(unique_columns).orderBy(F.lit(1))
    df_with_rn = on_time_df.withColumn("rn", F.row_number().over(window_spec))

    duplicate_count = df_with_rn.filter(F.col("rn") > 1).count()

    gx_df = SparkDFDataset(on_time_df)

    # CHIỀU 1: Completeness 
    gx_df.expect_column_values_to_not_be_null(column="lat", mostly=0.95)
    gx_df.expect_column_values_to_not_be_null(column="lng", mostly=0.95)
    gx_df.expect_column_values_to_not_be_null(column="cell_code", mostly=1.0)
    gx_df.expect_column_value_lengths_to_be_between(column="cell_code", min_value=1, mostly=1.0)

    # CHIỀU 2: Validity 
    gx_df.expect_column_values_to_be_between(column="p_cell_rsrp", min_value=-140, max_value=-44, mostly=0.98)
    gx_df.expect_column_values_to_be_between(column="p_cell_rsrq", min_value=-20, max_value=-3, mostly=0.98)

    # CHIỀU 3: Accuracy
    gx_df.expect_column_values_to_be_between(column="lat", min_value=8.5, max_value=23.4, mostly=0.95)
    gx_df.expect_column_values_to_be_between(column="lng", min_value=102.1, max_value=109.5, mostly=0.95)
    gx_df.expect_column_values_to_not_be_in_set(column="lat", value_set=[0.0], mostly=0.95)
    
    # CHIỀU 4: Consistency

    expected_date_hour = f"{args.date}-{args.hour}"
    gx_df.expect_column_values_to_be_in_set(column="date_hour", value_set=[expected_date_hour], mostly=1.0)
    
    # CHIỀU 5: Uniqueness
    gx_df.expect_compound_columns_to_be_unique(column_list=unique_columns, mostly=0.98)

    validation_result = gx_df.validate()
    
    statistics = validation_result["statistics"]
    success_rate = statistics["success_percent"]
    is_passed = validation_result["success"]
    
    failed_expectations_count = statistics["unsuccessful_expectations"]
    evaluated_expectations_count = statistics["evaluated_expectations"]

    print(f"\n=== BÁO CÁO CHẤT LƯỢNG DỮ LIỆU ===")
    print(f" [Điểm DQ (GX Rule Score)] : {success_rate:.2f}%")
    print(f"--------------------------------------------------\n")

    # BỘ LỌC ĐẨY SANG TẦNG SILVER
    clean_cond = (
        F.col("lat").isNotNull() & F.col("lng").isNotNull() & (F.col("lat") != 0.0) &
        F.col("lat").between(8.5, 23.4) & F.col("lng").between(102.1, 109.5) &
        F.col("p_cell_rsrp").between(-140, -44) & F.col("p_cell_rsrq").between(-20, -3) &
        (F.col("rn") == 1) 
    )

    clean_df = df_with_rn.filter(clean_cond).drop("rn", "batch_date", "batch_hour")
    dirty_df = df_with_rn.filter(~clean_cond).drop("rn", "batch_date", "batch_hour")

    clean_count = clean_df.count()
    dirty_count = dirty_df.count()

    replace_condition = f"date = '{args.date}' AND hour = '{args.hour}'"
    clean_df.withColumn("processed_at", F.current_timestamp()).write.format("delta").mode("overwrite").option("replaceWhere", replace_condition).partitionBy("date", "hour").save(clean_path)
    dirty_df.withColumn("processed_at", F.current_timestamp()).write.format("delta").mode("overwrite").option("replaceWhere", replace_condition).partitionBy("date", "hour").save(dirty_path)

    report_data = [
        (args.date, args.hour, total_raw_records, late_count, on_time_count, duplicate_count, clean_count, dirty_count, 
         float(success_rate), int(evaluated_expectations_count), int(failed_expectations_count), bool(is_passed), datetime.now(vn_tz))
    ]
    schema = """date string, hour string, total_raw_records long, late_count long, on_time_count long, duplicate_count long, clean_count long, dirty_count long, 
                gx_success_rate double, gx_total_rules long, gx_failed_rules long, gx_is_passed boolean, created_at timestamp"""
    
    report_df = spark.createDataFrame(report_data, schema=schema)
    report_df.write.format("delta").mode("overwrite").option("replaceWhere", replace_condition).partitionBy("date", "hour").save(report_path)

    THRESHOLD_SCORE = 80.0 
    if success_rate < THRESHOLD_SCORE:
        raise ValueError(f" CRITICAL DQ BLOCKER: Chất lượng lô dữ liệu ({success_rate:.2f}% < {THRESHOLD_SCORE}%)")

    print(" Task 2: Quy trình kiểm định chất lượng hoàn tất!")

if __name__ == "__main__":
    main()