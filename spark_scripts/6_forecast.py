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
    parser.add_argument("--lookback_days", type=int, default=7, help="Số ngày lịch sử để train")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(f"Multivariate_Prophet_Forecasting_{args.date}_{args.hour}_LB{args.lookback_days}")
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
        print(f"❌ Lỗi đọc bảng Delta: {e}")
        sys.exit(1)

    if df_fact.isEmpty():
        print("⚠️ Bảng dữ liệu trống, dừng luồng dự báo.")
        sys.exit(0)

    print(f"🔍 Lọc lấy dữ liệu lịch sử {args.lookback_days} ngày trước ngày {args.date}...")
    df_filtered = df_fact.filter(
        F.col("date") >= F.date_sub(F.lit(args.date), args.lookback_days)
    )

    df_prep = df_filtered.withColumn(
        "record_time", 
        F.to_timestamp(F.concat_ws(" ", F.col("date"), F.concat(F.col("hour"), F.lit(":00:00"))))
    ).select("h3_index", "cell_id", "record_time", "user_density_count", "avg_rsrp").dropna()

    forecast_schema = StructType([
        StructField("h3_index", StringType(), True),
        StructField("cell_id", StringType(), True),
        StructField("forecast_time", TimestampType(), True),
        StructField("forecast_density", DoubleType(), True),
        StructField("forecast_lower", DoubleType(), True),
        StructField("forecast_upper", DoubleType(), True),
        StructField("forecast_rsrp", DoubleType(), True),
        StructField("forecast_rsrp_lower", DoubleType(), True),
        StructField("forecast_rsrp_upper", DoubleType(), True)
    ])

    def forecast_multivariate(history_pd: pd.DataFrame) -> pd.DataFrame:
        # =======================================================
        # BỊT MIỆNG PROPHET & STAN ĐỂ CỨU AIRFLOW WORKER KHỎI CRASH
        # =======================================================
        import logging
        logging.getLogger('prophet').setLevel(logging.ERROR)
        logging.getLogger('cmdstanpy').disabled = True
        
        if len(history_pd) < 24:
            return pd.DataFrame(columns=[f.name for f in forecast_schema.fields])

        history_pd = history_pd.sort_values('record_time')
        last_dt = history_pd['record_time'].max()

        # LUỒNG 1: DỰ BÁO USER DENSITY (uncertainty_samples=10)
        df_train_density = history_pd[['record_time', 'user_density_count']].rename(columns={'record_time': 'ds', 'user_density_count': 'y'})
        m_density = Prophet(daily_seasonality=True, weekly_seasonality=False, yearly_seasonality=False, uncertainty_samples=10)
        m_density.fit(df_train_density)
        future_density = m_density.make_future_dataframe(periods=24, freq='H')
        forecast_density = m_density.predict(future_density)
        future_out_density = forecast_density[forecast_density['ds'] > last_dt].copy()
        
        future_out_density['yhat'] = future_out_density['yhat'].clip(lower=0)
        future_out_density['yhat_lower'] = future_out_density['yhat_lower'].clip(lower=0)
        future_out_density['yhat_upper'] = future_out_density['yhat_upper'].clip(lower=0)

        # LUỒNG 2: DỰ BÁO RSRP (uncertainty_samples=10)
        df_train_rsrp = history_pd[['record_time', 'avg_rsrp']].rename(columns={'record_time': 'ds', 'avg_rsrp': 'y'})
        m_rsrp = Prophet(daily_seasonality=True, weekly_seasonality=False, yearly_seasonality=False, uncertainty_samples=10)
        m_rsrp.fit(df_train_rsrp)
        future_rsrp = m_rsrp.make_future_dataframe(periods=24, freq='H')
        forecast_rsrp = m_rsrp.predict(future_rsrp)
        future_out_rsrp = forecast_rsrp[forecast_rsrp['ds'] > last_dt].copy()
        
        future_out_rsrp['yhat'] = future_out_rsrp['yhat'].clip(lower=-140, upper=-40)
        future_out_rsrp['yhat_lower'] = future_out_rsrp['yhat_lower'].clip(lower=-140, upper=-40)
        future_out_rsrp['yhat_upper'] = future_out_rsrp['yhat_upper'].clip(lower=-140, upper=-40)

        output_pd = pd.DataFrame()
        output_pd['forecast_time'] = future_out_density['ds']
        output_pd['forecast_density'] = future_out_density['yhat']
        output_pd['forecast_lower'] = future_out_density['yhat_lower']
        output_pd['forecast_upper'] = future_out_density['yhat_upper']
        output_pd['forecast_rsrp'] = future_out_rsrp['yhat']
        output_pd['forecast_rsrp_lower'] = future_out_rsrp['yhat_lower']
        output_pd['forecast_rsrp_upper'] = future_out_rsrp['yhat_upper']
        output_pd['h3_index'] = history_pd['h3_index'].iloc[0]
        output_pd['cell_id'] = history_pd['cell_id'].iloc[0]

        return output_pd[[f.name for f in forecast_schema.fields]]

    print("⚡ Đang chạy Huấn luyện Kép Prophet trên toàn cụm Spark...")
    forecast_df = (
        df_prep.repartition("h3_index", "cell_id") 
        .groupBy("h3_index", "cell_id")
        .applyInPandas(forecast_multivariate, schema=forecast_schema)
    )

    final_forecast_df = forecast_df.withColumn("forecast_density", F.round("forecast_density").cast("int")) \
                                   .withColumn("forecast_date", F.to_date("forecast_time")) \
                                   .withColumn("forecast_hour", F.date_format("forecast_time", "HH"))

    print("💾 Đang cập nhật dữ liệu dự báo xuống Delta Lake Gold...")
    final_forecast_df.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("forecast_date", "forecast_hour") \
        .save(forecast_output_path)

    print("✅ Job 6: Dự báo Kép Prophet hoàn tất!")

if __name__ == "__main__":
    main()