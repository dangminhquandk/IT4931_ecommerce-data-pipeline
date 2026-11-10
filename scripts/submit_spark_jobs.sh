#!/usr/bin/env bash
# ==============================================================================
# IT4931 - LƯU TRỮ VÀ XỬ LÝ DỮ LIỆU LỚN (NHÓM 3)
# Script khởi tạo dữ liệu HDFS và submit các tác vụ Spark Batch, Streaming & Benchmark
# ==============================================================================
set -e

echo "[1/4] Generating >1,000,000 historical e-commerce transactions..."
python3 -m src.ingestion.historical_data_loader --rows 1050000

echo "[2/4] Uploading Raw CSV & Clickstream JSON to HDFS Raw Layer (BlockSize=128MB, Replication=1)..."
docker exec hdfs-namenode-service hdfs dfs -mkdir -p /data/raw/transactions_csv /data/raw/products_csv /data/raw/clickstream_json /data/refined /data/partitioned /data/checkpoints
docker exec hdfs-namenode-service hdfs dfs -D dfs.blocksize=134217728 -D dfs.replication=1 -put -f /data/raw/transactions_csv/transactions.csv /data/raw/transactions_csv/
docker exec hdfs-namenode-service hdfs dfs -D dfs.blocksize=134217728 -D dfs.replication=1 -put -f /data/raw/products_csv/products_catalog.csv /data/raw/products_csv/

echo "[3/4] Running Spark Batch Analytics, Parquet/Snappy Partitioning & SQL Benchmark..."
docker exec spark-master-service python3 /app/scripts/run_local_demo.py --rows 1050000

echo "[4/4] Submitting Spark Structured Streaming Job (Sliding Window 1m/10s, Watermark 10m)..."
docker exec -d spark-master-service spark-submit \
  --master spark://spark-master-service:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  /app/src/streaming/run_spark_stream_job.py

echo "All pipeline jobs submitted! Open Streamlit Dashboard at http://localhost:8501"

