import sys
import os
import json
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pyspark.sql.functions as F

def main():
    spark = (
        SparkSession.builder.appName("Process_Late_Arrival_Data_To_Silver")
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

    late_arrival_path = "s3a://silver/late_arrival_data/"
    silver_target_path = "s3a://silver/mdt_clean/" # MERGE VÀO LỚP SẠCH SILVER

    print("=== BẮT ĐẦU XỬ LÝ DỮ LIỆU ĐẾN TRỄ VÀO TẦNG SILVER ===")

    try:
        late_df = spark.read.format("delta").load(late_arrival_path)
    except Exception as e:
        print(f" Không có bảng Delta tại {late_arrival_path}. Kết thúc an toàn.")
        sys.exit(0)

    if late_df.isEmpty():
        print(" Bảng Late Arrival không có dữ liệu. Kết thúc luồng.")
        sys.exit(0)

    # 1. Tiền xử lý dữ liệu trễ
    unique_columns = ["imsi", "time_ms", "lat", "lng", "gcell_code", "nr_rsrp", "nr_rsrq"]
    late_df_clean = late_df.dropDuplicates(unique_columns)

    # 2. Lấy danh sách các phân vùng (ca log) bị ảnh hưởng để Lát nữa chạy lại Job 3
    affected_partitions = late_df_clean.select("date", "hour").distinct().collect()
    partitions_list = [{"date": row["date"], "hour": row["hour"]} for row in affected_partitions]

    print(f" Phát hiện dữ liệu trễ thuộc về {len(partitions_list)} ca log cũ: {partitions_list}")

    # 3. Load bảng Silver và thực hiện MERGE
    try:
        target_table = DeltaTable.forPath(spark, silver_target_path)
    except Exception as e:
        print(f" Lỗi: Chưa tồn tại bảng đích tại {silver_target_path}.")
        sys.exit(1)

    print(f" Đang MERGE {late_df_clean.count()} bản ghi trễ vào tầng SILVER...")

    target_table.alias("target").merge(
        late_df_clean.alias("source"),
        """
        target.date = source.date AND 
        target.hour = source.hour AND 
        target.imsi = source.imsi AND 
        target.time_ms = source.time_ms
        """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # 4. Ghi danh sách các ca bị ảnh hưởng ra file JSON để Airflow đọc và trigger Job 3
    # Mẹo: Ghi ra một file tạm trên container
    with open('/tmp/affected_partitions.json', 'w') as f:
        json.dumps(partitions_list, f)

    # 5. Dọn dẹp khu vực cách ly
    try:
        delta_late = DeltaTable.forPath(spark, late_arrival_path)
        delta_late.delete() 
        print(" Đã dọn dẹp thành công thư mục Quarantine.")
    except Exception as e:
        pass

    print("=== JOB 4 HOÀN TẤT. VUI LÒNG RE-RUN JOB 3 CHO CÁC CA BỊ ẢNH HƯỞNG! ===")

if __name__ == "__main__":
    main()