import sys
import argparse
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from prophet import Prophet

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Ngày chạy dự báo (YYYY-MM-DD)")
    parser.add_argument("--hour", required=True, help="Giờ chạy dự báo (HH)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(f"Multivariate_Prophet_Forecasting_{args.date}_{args.hour}")
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

    print("🚀 Đang nạp dữ liệu lịch sử từ Fact MDT Hourly...")
    try:
        df_fact = spark.read.format("delta").load(gold_fact_path)
    except Exception as e:
        print(f" Lỗi đọc bảng Delta: {e}")
        sys.exit(1)

    if df_fact.isEmpty():
        print(" Bảng dữ liệu trống, dừng luồng dự báo.")
        sys.exit(0)

    # 1. Tạo cột Timestamp chuẩn cho Prophet & Chọn thêm RSRP
    df_prep = df_fact.withColumn(
        "record_time", 
        F.to_timestamp(F.concat_ws(" ", F.col("date"), F.concat(F.col("hour"), F.lit(":00:00"))))
    ).select("h3_index", "cell_id", "record_time", "user_density_count", "avg_rsrp").dropna()

    # 2. SCHEMA MỚI: Bổ sung 3 cột RSRP tương lai
    forecast_schema = StructType([
        StructField("h3_index", StringType(), True),
        StructField("cell_id", StringType(), True),
        StructField("forecast_time", TimestampType(), True),
        StructField("forecast_density", DoubleType(), True),
        StructField("forecast_lower", DoubleType(), True),
        StructField("forecast_upper", DoubleType(), True),
        # --- Cột mới thêm ---
        StructField("forecast_rsrp", DoubleType(), True),
        StructField("forecast_rsrp_lower", DoubleType(), True),
        StructField("forecast_rsrp_upper", DoubleType(), True)
    ])

    # 3. PANDAS UDF: Chạy 2 luồng Prophet song song cho từng Cell
    def forecast_multivariate(history_pd: pd.DataFrame) -> pd.DataFrame:
        if len(history_pd) < 24:
            return pd.DataFrame(columns=[f.name for f in forecast_schema.fields])

        # Sắp xếp thời gian
        history_pd = history_pd.sort_values('record_time')
        last_dt = history_pd['record_time'].max()

        # ==========================================
        # LUỒNG 1: DỰ BÁO USER DENSITY
        # ==========================================
        df_train_density = history_pd[['record_time', 'user_density_count']].rename(columns={'record_time': 'ds', 'user_density_count': 'y'})
        
        m_density = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=False)
        m_density.fit(df_train_density)
        
        future_density = m_density.make_future_dataframe(periods=24, freq='H')
        forecast_density = m_density.predict(future_density)
        
        # Chỉ lấy tương lai & Cắt số âm
        future_out_density = forecast_density[forecast_density['ds'] > last_dt].copy()
        future_out_density['yhat'] = future_out_density['yhat'].clip(lower=0)
        future_out_density['yhat_lower'] = future_out_density['yhat_lower'].clip(lower=0)

        # ==========================================
        # LUỒNG 2: DỰ BÁO RSRP
        # ==========================================
        df_train_rsrp = history_pd[['record_time', 'avg_rsrp']].rename(columns={'record_time': 'ds', 'avg_rsrp': 'y'})
        
        # RSRP thường biến động ít hơn số người, ta dùng chung cấu hình Prophet
        m_rsrp = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=False)
        m_rsrp.fit(df_train_rsrp)
        
        future_rsrp = m_rsrp.make_future_dataframe(periods=24, freq='H')
        forecast_rsrp = m_rsrp.predict(future_rsrp)
        
        # Chỉ lấy tương lai & Giới hạn RSRP lý thuyết (-140 đến -40 dBm)
        future_out_rsrp = forecast_rsrp[forecast_rsrp['ds'] > last_dt].copy()
        future_out_rsrp['yhat'] = future_out_rsrp['yhat'].clip(lower=-140, upper=-40)
        future_out_rsrp['yhat_lower'] = future_out_rsrp['yhat_lower'].clip(lower=-140, upper=-40)
        future_out_rsrp['yhat_upper'] = future_out_rsrp['yhat_upper'].clip(lower=-140, upper=-40)

        # ==========================================
        # GỘP KẾT QUẢ ĐẦU RA (MERGE)
        # ==========================================
        output_pd = pd.DataFrame()
        output_pd['forecast_time'] = future_out_density['ds']
        
        # Gắn dữ liệu Density
        output_pd['forecast_density'] = future_out_density['yhat']
        output_pd['forecast_lower'] = future_out_density['yhat_lower']
        output_pd['forecast_upper'] = future_out_density['yhat_upper']
        
        # Gắn dữ liệu RSRP
        output_pd['forecast_rsrp'] = future_out_rsrp['yhat']
        output_pd['forecast_rsrp_lower'] = future_out_rsrp['yhat_lower']
        output_pd['forecast_rsrp_upper'] = future_out_rsrp['yhat_upper']

        # Gắn khóa định danh
        output_pd['h3_index'] = history_pd['h3_index'].iloc[0]
        output_pd['cell_id'] = history_pd['cell_id'].iloc[0]

        return output_pd[[f.name for f in forecast_schema.fields]]

    # 4. Kích hoạt phân tán Cluster
    print("🤖 Đang chạy Huấn luyện Kép Prophet (Density + RSRP) trên toàn cụm...")
    forecast_df = (
        df_prep.groupBy("h3_index", "cell_id")
        .applyInPandas(forecast_multivariate, schema=forecast_schema)
    )

    final_forecast_df = forecast_df.withColumn("forecast_density", F.round("forecast_density").cast("int")) \
                                   .withColumn("forecast_date", F.to_date("forecast_time")) \
                                   .withColumn("forecast_hour", F.date_format("forecast_time", "HH"))

    # 5. GHI ĐÈ KẾT QUẢ XUỐNG DELTA
    # Lưu ý: Bắt buộc dùng overwriteSchema vì bảng mới sinh ra thêm 3 cột RSRP
    print(" Đang cập nhật dữ liệu đa biến xuống Delta Lake Gold...")
    final_forecast_df.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("forecast_date", "forecast_hour") \
        .save(forecast_output_path)

    print(" Job 6: Dự báo Kép Prophet hoàn tất xuất sắc!")

if __name__ == "__main__":
    main()