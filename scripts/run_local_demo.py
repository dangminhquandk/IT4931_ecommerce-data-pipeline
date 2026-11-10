"""
End-to-End Pipeline Runner & Experimental Benchmark Orchestrator (IT4931 - Group 3).
Executes all 4 Core Layers + Section IV SQL Benchmark:
1. Core 1 (Ingestion & 3-Tier Storage):
   - Generates >1,000,000 historical transactions CSV + products catalog CSV + clickstream JSON
   - Cleanses dirty records (Veracity) and writes Unpartitioned & Partitioned Parquet (Snappy)
   - Evaluates Row Group sizes (32MB, 64MB, 128MB, 256MB)
2. Core 2 (Batch Layer):
   - Computes Monthly/Category Revenue, Top 10 Products, Order Status, and K-Means RFM Customer Segments
3. Core 3 (Speed Layer):
   - Processes Clickstream events with 1-minute / 10-second Sliding Windows & 10-minute Watermarking
4. Core 4 & Section IV (Serving Layer + SQL Benchmark):
   - Runs the 4-query SQL Benchmark across (1) Raw CSV, (2) Unpartitioned Parquet, (3) Partitioned Parquet
   - Persists all Batch Views, Speed Views, and Benchmark metrics to PostgreSQL (or local SQLite fallback)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.batch.batch_processor import BatchProcessor
from src.benchmark.sql_benchmark import SQLBenchmarkRunner
from src.config import LOCAL_DATA_DIR
from src.ingestion.event_generator import ClickstreamEventGenerator
from src.ingestion.historical_data_loader import prepare_local_raw_datasets
from src.serving.db_client import ServingDatabaseClient
from src.storage.hdfs_storage_manager import HDFSStorageManager
from src.streaming.stream_processor import ClickstreamStreamProcessor


def run_pipeline(num_transactions: int = 1_050_000, num_stream_events: int = 3600, force: bool = False) -> None:
    t_start = time.perf_counter()
    db_client = ServingDatabaseClient()
    logging.info("==========================================================================")
    logging.info("IT4931 - E-COMMERCE BIG DATA PIPELINE & BENCHMARK ORCHESTRATOR (GROUP 3)")
    logging.info("Serving Database Backend: %s", db_client.backend_name)
    logging.info("==========================================================================")

    # 1. Core 1: Data Ingestion (Historical Batch CSV + Real-time Clickstream JSON)
    logging.info("[Step 1/5] Generating / Loading Raw Datasets (>1M Historical CSV + Clickstream JSON)...")
    tx_csv_path, prod_csv_path, _ = prepare_local_raw_datasets(
        num_records=num_transactions,
        force_regenerate=force,
    )
    event_gen = ClickstreamEventGenerator(seed=42)
    click_events = event_gen.generate_batch(count=num_stream_events, step_ms=100)
    click_json_path = event_gen.save_jsonl_to_raw_layer(click_events)

    # 2. Core 1: 3-Layer HDFS/Parquet Storage Transformation & Row Group Experiment
    logging.info("[Step 2/5] Executing Raw -> Refined (Parquet/Snappy) -> Partitioned (year=/month=/) ETL...")
    storage_mgr = HDFSStorageManager()
    clean_tx_df, quality_df, storage_comp_df = (
        storage_mgr.transform_raw_to_refined_and_partitioned_pyarrow(
            raw_tx_csv_path=tx_csv_path,
            raw_prod_csv_path=prod_csv_path,
            raw_click_json_path=click_json_path,
            base_data_dir=LOCAL_DATA_DIR,
        )
    )
    db_client.write_dataframe(quality_df, "data_quality_report", if_exists="replace")
    db_client.write_dataframe(storage_comp_df, "storage_comparison", if_exists="replace")

    logging.info("[Step 2b/5] Evaluating Parquet Row Group Sizes (32MB, 64MB, 128MB, 256MB)...")
    rg_df = storage_mgr.evaluate_row_group_sizes(clean_tx_df, base_data_dir=LOCAL_DATA_DIR)
    db_client.write_dataframe(rg_df, "rowgroup_experiments", if_exists="replace")

    # 3. Core 2: Batch Processing Layer
    logging.info("[Step 3/5] Running Batch Processing Layer (Revenue, Top 10 Products, RFM K-Means)...")
    import pandas as pd

    products_df = pd.read_csv(prod_csv_path)
    batch_proc = BatchProcessor(db_client=db_client)
    batch_proc.run_local_batch_analytics(clean_tx_df=clean_tx_df, products_df=products_df)

    # 4. Core 3: Speed Processing Layer (Sliding Window 1m/10s + Watermark 10m)
    logging.info("[Step 4/5] Running Speed Processing Layer (Sliding Window 1m/10s + 10m Watermarking)...")
    stream_proc = ClickstreamStreamProcessor(db_client=db_client)
    stream_proc.process_clickstream_events_windowed(click_events)

    # 5. Section IV: SQL Benchmark Workload (Raw CSV vs Unpartitioned Parquet vs Partitioned Parquet)
    logging.info("[Step 5/5] Executing SQL Benchmark Workload across 3 Storage Architectures...")
    benchmark_runner = SQLBenchmarkRunner(db_client=db_client)
    bench_df = benchmark_runner.run_pyarrow_real_benchmark(base_data_dir=LOCAL_DATA_DIR)

    elapsed = time.perf_counter() - t_start
    logging.info("==========================================================================")
    logging.info("PIPELINE COMPLETED SUCCESSFULLY in %.2f seconds!", elapsed)
    logging.info("Storage Comparison Summary:\n%s", storage_comp_df[["storage_layer", "total_size_mb", "compression_ratio"]].to_string(index=False))
    logging.info(
        "SQL Benchmark Summary:\n%s",
        bench_df[
            [
                "query_id",
                "storage_architecture",
                "execution_time_sec",
                "bytes_read_mb",
                "speedup_vs_csv",
                "io_reduction_pct",
            ]
        ].to_string(index=False),
    )
    logging.info("==========================================================================")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Run E-Commerce Big Data Pipeline & SQL Benchmark")
    parser.add_argument("--rows", type=int, default=1_050_000, help="Number of historical transactions")
    parser.add_argument("--events", type=int, default=3600, help="Number of clickstream events")
    parser.add_argument("--force", action="store_true", help="Force regeneration of raw CSV files")
    args = parser.parse_args()
    run_pipeline(num_transactions=args.rows, num_stream_events=args.events, force=args.force)

