"""
Automated Unit & Integration Tests for the Real-Time E-Commerce Data Pipeline (IT4931 - Group 3).
Verifies:
1. Core 1: Historical Data Loader, Kaggle Converter, and Clickstream Event Generator
2. Core 1: 3-Layer Storage (Raw CSV/JSON -> Refined Parquet/Snappy -> Partitioned year=/month=/)
3. Core 2: Batch Layer Analytics (Revenue, Top 10 Products Join, RFM K-Means Segmentation)
4. Core 3: Speed Layer Sliding Window (1m/10s) & Watermarking (10m Late Data filtering)
5. Section IV: SQL Benchmark across (1) Raw CSV, (2) Unpartitioned Parquet, (3) Partitioned Parquet
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.batch.batch_processor import BatchProcessor
from src.benchmark.sql_benchmark import SQLBenchmarkRunner
from src.config import DatabaseConfig
from src.ingestion.event_generator import ClickstreamEventGenerator
from src.ingestion.historical_data_loader import (
    generate_historical_transactions,
    generate_products_catalog,
)
from src.serving.db_client import ServingDatabaseClient
from src.storage.hdfs_storage_manager import HDFSStorageManager
from src.streaming.stream_processor import ClickstreamStreamProcessor


def test_end_to_end_pipeline_and_benchmark(tmp_path: Path) -> None:
    # 1. Setup temporary Serving Layer DB (SQLite fallback)
    db_cfg = DatabaseConfig(sqlite_fallback_path=tmp_path / "test_serving.db")
    db = ServingDatabaseClient(config=db_cfg, force_sqlite=True)

    # 2. Test Historical Data Generation (with dirty records for Veracity testing)
    prod_df = generate_products_catalog(num_products=100, seed=42)
    tx_df, stats = generate_historical_transactions(
        num_records=15_000,
        num_customers=1_200,
        products_df=prod_df,
        inject_dirty_rate=0.02,
        seed=42,
    )
    assert len(tx_df) == 15_000
    assert set(tx_df.columns) == {
        "Order_ID",
        "Customer_ID",
        "Product_ID",
        "Price",
        "Category",
        "Order_Status",
        "Timestamp",
    }
    assert stats["invalid_price_injected"] > 0

    raw_tx_csv = tmp_path / "raw" / "transactions_csv" / "transactions.csv"
    raw_prod_csv = tmp_path / "raw" / "products_csv" / "products_catalog.csv"
    raw_tx_csv.parent.mkdir(parents=True, exist_ok=True)
    raw_prod_csv.parent.mkdir(parents=True, exist_ok=True)
    tx_df.to_csv(raw_tx_csv, index=False)
    prod_df.to_csv(raw_prod_csv, index=False)

    # 3. Test Clickstream Event Generator
    gen = ClickstreamEventGenerator(seed=42)
    events = gen.generate_batch(count=600, step_ms=200)
    assert len(events) == 600
    assert {"user_id", "product_id", "event_type", "session_id", "timestamp"}.issubset(
        events[0].keys()
    )
    raw_click_json = tmp_path / "raw" / "clickstream_json" / "clickstream_events.json"
    gen.save_jsonl_to_raw_layer(events, output_path=raw_click_json)

    # 4. Test 3-Layer Storage Transformation (Raw -> Refined Parquet/Snappy -> Partitioned)
    storage_mgr = HDFSStorageManager()
    clean_df, quality_df, storage_comp_df = (
        storage_mgr.transform_raw_to_refined_and_partitioned_pyarrow(
            raw_tx_csv_path=raw_tx_csv,
            raw_prod_csv_path=raw_prod_csv,
            raw_click_json_path=raw_click_json,
            base_data_dir=tmp_path,
        )
    )
    # Verify dirty records were cleaned
    assert len(clean_df) < 15_000
    assert (clean_df["Price"] > 0).all()
    assert clean_df["Category"].isna().sum() == 0
    assert clean_df["Order_ID"].is_unique

    # Verify Parquet compression & Partitioned directory layout (year=/month=/)
    assert len(storage_comp_df) == 3
    part_dir = tmp_path / "partitioned" / "transactions_parquet_partitioned" / "year=2025"
    assert part_dir.exists()
    click_part_dir = tmp_path / "partitioned" / "clickstream_parquet_partitioned" / "year=2026"
    assert click_part_dir.exists() or any(
        (tmp_path / "partitioned" / "clickstream_parquet_partitioned").glob("year=*")
    )

    # Verify Row Group Size evaluation
    rg_df = storage_mgr.evaluate_row_group_sizes(clean_df, base_data_dir=tmp_path)
    assert list(rg_df["row_group_size_mb"]) == [32, 64, 128, 256]

    # 5. Test Batch Layer Analytics & K-Means RFM Customer Segmentation
    batch_proc = BatchProcessor(db_client=db)
    views = batch_proc.run_local_batch_analytics(clean_tx_df=clean_df, products_df=prod_df)
    assert len(views["batch_top_products"]) == 10
    assert len(views["batch_customer_segments"]) == 4
    assert not db.read_table("batch_revenue_summary").empty

    # 6. Test Speed Layer Sliding Window (1m/10s) & Watermarking (10m)
    stream_proc = ClickstreamStreamProcessor(db_client=db)
    win_df, cat_df = stream_proc.process_clickstream_events_windowed(
        events, watermark_minutes=10, window_seconds=60, slide_seconds=10
    )
    assert not win_df.empty
    assert not cat_df.empty

    # 7. Test SQL Benchmark across 3 Storage Architectures
    bench_runner = SQLBenchmarkRunner(db_client=db)
    bench_df = bench_runner.run_pyarrow_real_benchmark(base_data_dir=tmp_path)
    assert len(bench_df) == 12  # 4 queries x 3 storage architectures

    # Verify Partitioned Parquet reduces Bytes Read compared to Raw CSV on Q1 (Partition Pruning)
    q1_rows = bench_df[bench_df["query_id"] == "Q1_Partition_Pruning"]
    csv_bytes = float(
        q1_rows[q1_rows["storage_architecture"].str.contains("CSV")]["bytes_read_mb"].iloc[0]
    )
    part_bytes = float(
        q1_rows[q1_rows["storage_architecture"].str.contains("Partitioned Parquet")][
            "bytes_read_mb"
        ].iloc[0]
    )
    assert part_bytes < csv_bytes

