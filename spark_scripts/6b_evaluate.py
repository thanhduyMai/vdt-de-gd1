import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def evaluate_metrics():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval_date", required=True, help="Ngày cần đánh giá (YYYY-MM-DD)")
    args = parser.parse_args()

    # Bổ sung đầy đủ config Delta Lake để Spark có quyền Write
    spark = (
        SparkSession.builder.appName(f"Evaluate_Forecast_Metrics_{args.eval_date}")
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

    gold_fact_path = "s3a://gold/fact_mdt_hourly/"
    forecast_output_path = "s3a://gold/fact_forecast_hourly/"
    evaluation_output_path = "s3a://gold/fact_model_evaluation/" # ĐƯỜNG DẪN MỚI LƯU METRICS

    print(f"🚀 Đang khởi động luồng đánh giá mô hình cho ngày {args.eval_date}...")

    
    # 1. Đọc Thực tế VÀ TẠO CỘT RECORD_TIME ĐỂ JOIN
    df_actual = spark.read.format("delta").load(gold_fact_path) \
        .filter(F.col("date") == args.eval_date) \
        .withColumnRenamed("user_density_count", "actual_density") \
        .withColumnRenamed("avg_rsrp", "actual_rsrp") \
        .withColumn("record_time", F.to_timestamp(F.concat_ws(" ", F.col("date"), F.concat(F.col("hour"), F.lit(":00:00")))))

    # 2. Đọc Dự báo
    df_forecast = spark.read.format("delta").load(forecast_output_path) \
        .filter(F.col("forecast_date") == args.eval_date) \
        .withColumnRenamed("forecast_time", "record_time")

    if df_actual.isEmpty() or df_forecast.isEmpty():
        print("⚠️ Thiếu dữ liệu thực tế hoặc dự báo để đánh giá. Dừng an toàn.")
        return

    # 3. JOIN 2 bảng
    df_join = df_actual.join(
        df_forecast,
        on=["h3_index", "cell_id", "record_time"],
        how="inner"
    )

    # 4. TÍNH TOÁN CÁC ĐỘ LỆCH (ERRORS) DÒNG TỪNG DÒNG
    df_error = df_join.withColumn(
        "err_density", F.col("actual_density") - F.col("forecast_density")
    ).withColumn(
        "err_rsrp", F.col("actual_rsrp") - F.col("forecast_rsrp")
    )

    df_metrics_raw = df_error.withColumn(
        "abs_err_density", F.abs(F.col("err_density"))
    ).withColumn(
        "sq_err_density", F.pow(F.col("err_density"), 2)
    ).withColumn(
        "mape_density",
        F.when(F.col("actual_density") == 0, 0.0)
         .otherwise(F.abs(F.col("err_density")) / F.col("actual_density") * 100)
    ).withColumn(
        "abs_err_rsrp", F.abs(F.col("err_rsrp"))
    ).withColumn(
        "sq_err_rsrp", F.pow(F.col("err_rsrp"), 2)
    )

    # 5. AGGREGATE THEO TỪNG TRẠM (CELL_ID) VÀ KHU VỰC
    df_cell_metrics = df_metrics_raw.groupBy("h3_index", "cell_id").agg(
        F.lit(args.eval_date).alias("eval_date"),
        F.round(F.avg("abs_err_density"), 2).alias("mae_density"),
        F.round(F.sqrt(F.avg("sq_err_density")), 2).alias("rmse_density"),
        F.round(F.avg("mape_density"), 2).alias("mape_density"),
        F.round(F.avg("abs_err_rsrp"), 2).alias("mae_rsrp"),
        F.round(F.sqrt(F.avg("sq_err_rsrp")), 2).alias("rmse_rsrp"),
        F.count("*").alias("total_matched_records")
    )

    # 6. GHI DỮ LIỆU XUỐNG DELTA LAKE (Dùng replaceWhere để an toàn khi backfill)
    print("💾 Đang ghi kết quả đánh giá (Metrics) xuống Delta Lake...")
    df_cell_metrics.write.format("delta").mode("overwrite") \
        .option("replaceWhere", f"eval_date = '{args.eval_date}'") \
        .option("mergeSchema", "true") \
        .partitionBy("eval_date") \
        .save(evaluation_output_path)

    print(f"✅ Hoàn tất lưu Metrics Đánh giá mô hình ngày {args.eval_date}!")

if __name__ == "__main__":
    evaluate_metrics()