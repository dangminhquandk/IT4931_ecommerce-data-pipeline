"""
Speed Processing Layer - Spark Structured Streaming (Core 3).
Reference:
- Proposal Section III.3:
  - Event-Time Processing over Kafka topic `clickstream-events`
  - Sliding Window: 1 minute duration, sliding every 10 seconds
  - Watermarking: 10 minutes threshold to handle Late Data and prevent State Store memory leaks
  - Real-time metrics: event count, interacting user count, transaction/purchase count per minute
- IT4931 Note 5 (Kafka) & Note 8 (Lambda Architecture):
  - Speed Layer compensates for the processing lag of the Batch Layer
  - Writes incremental Real-Time Views to the PostgreSQL Serving Layer
  - Simultaneously archives clickstream events to HDFS Partitioned Layer (`year=/month=/event_type=/`)
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

from src.config import (
    HDFS_CONFIG,
    KAFKA_CONFIG,
    STREAMING_CONFIG,
    HDFSConfig,
    KafkaConfig,
    StreamingConfig,
)
from src.serving.db_client import ServingDatabaseClient
from src.storage.hdfs_storage_manager import build_spark_session

logger = logging.getLogger(__name__)


class ClickstreamStreamProcessor:
    """
    Spark Structured Streaming processor for real-time e-commerce clickstream analytics
    with Event-Time Sliding Windows (1m window, 10s slide) and Watermarking (10m).
    """

    def __init__(
        self,
        hdfs_config: HDFSConfig = HDFS_CONFIG,
        kafka_config: KafkaConfig = KAFKA_CONFIG,
        streaming_config: StreamingConfig = STREAMING_CONFIG,
        db_client: ServingDatabaseClient | None = None,
    ) -> None:
        self.hdfs_config = hdfs_config
        self.kafka_config = kafka_config
        self.streaming_config = streaming_config
        self.db_client = db_client or ServingDatabaseClient()

    def start_spark_structured_streaming(self, spark: Any | None = None) -> None:
        """
        Launch the continuous Spark Structured Streaming query consuming from Kafka
        `clickstream-events`, applying a 10-minute Watermark and 1-minute / 10-second Sliding Window.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        if spark is None:
            spark = build_spark_session("IT4931-Speed-Layer-Streaming", self.hdfs_config)

        event_schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("session_id", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("is_late", BooleanType(), True),
                StructField("delay_seconds", IntegerType(), True),
            ]
        )

        raw_kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_config.bootstrap_servers)
            .option("subscribe", self.kafka_config.topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed_stream = (
            raw_kafka_stream.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_payload")
            .select(F.from_json(F.col("json_payload"), event_schema).alias("data"))
            .select("data.*")
            .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .filter(F.col("event_timestamp").isNotNull())
            .withColumn("year", F.year("event_timestamp"))
            .withColumn("month", F.month("event_timestamp"))
        )

        # Apply Watermarking (10 minutes) + Sliding Window (1 minute duration, 10 seconds slide)
        windowed_metrics = (
            parsed_stream.withWatermark("event_timestamp", self.streaming_config.watermark_delay)
            .groupBy(
                F.window(
                    F.col("event_timestamp"),
                    self.streaming_config.window_duration,
                    self.streaming_config.slide_duration,
                )
            )
            .agg(
                F.count("event_id").alias("total_events"),
                F.approx_count_distinct("user_id").alias("active_users"),
                F.approx_count_distinct("session_id").alias("active_sessions"),
                F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
                F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
                F.sum(F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)).alias(
                    "remove_cart_count"
                ),
                F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                F.round(
                    F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)),
                    2,
                ).alias("gross_sales"),
                F.sum(
                    F.when(
                        F.col("is_late") & (F.col("delay_seconds") <= 600),
                        1,
                    ).otherwise(0)
                ).alias("late_events_accepted"),
                F.sum(
                    F.when(
                        F.col("is_late") & (F.col("delay_seconds") > 600),
                        1,
                    ).otherwise(0)
                ).alias("late_events_dropped"),
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "total_events",
                "active_users",
                "active_sessions",
                "view_count",
                "cart_count",
                "remove_cart_count",
                "purchase_count",
                "gross_sales",
                F.round(
                    F.col("purchase_count") * 100.0 / F.greatest(F.lit(1), F.col("view_count")),
                    2,
                ).alias("conversion_rate_pct"),
                "late_events_accepted",
                "late_events_dropped",
            )
        )

        def write_window_microbatch(batch_df: Any, batch_id: int) -> None:
            pdf = batch_df.toPandas()
            if not pdf.empty:
                self.db_client.write_dataframe(pdf, "streaming_window_metrics", if_exists="append")
                logger.info("Micro-batch %d: wrote %d sliding windows to Serving Layer.", batch_id, len(pdf))

        # 1. Sink to PostgreSQL Serving Layer
        serving_query = (
            windowed_metrics.writeStream.outputMode("update")
            .foreachBatch(write_window_microbatch)
            .trigger(processingTime=self.streaming_config.trigger_interval)
            .start()
        )

        # 2. Sink to HDFS Partitioned Layer (year=/month=/event_type=/) with Checkpointing
        hdfs_query = (
            parsed_stream.withWatermark("event_timestamp", self.streaming_config.watermark_delay)
            .writeStream.format("parquet")
            .outputMode("append")
            .option("path", self.hdfs_config.partitioned_clickstream_path)
            .option("checkpointLocation", self.hdfs_config.streaming_checkpoint_path)
            .option("compression", self.hdfs_config.parquet_compression)
            .partitionBy("year", "month", "event_type")
            .trigger(processingTime="30 seconds")
            .start()
        )

        serving_query.awaitTermination()
        hdfs_query.awaitTermination()

    def process_clickstream_events_windowed(
        self,
        events: List[Dict[str, Any]],
        watermark_minutes: int = 10,
        window_seconds: int = 60,
        slide_seconds: int = 10,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Execute the exact Event-Time Sliding Window (1-minute window, 10-second slide)
        and Watermarking (10-minute threshold) logic over a stream of clickstream events.
        - Events with `delay_seconds > watermark_minutes * 60` are dropped by the Watermark
          State Store filter (simulating Spark Structured Streaming watermark eviction).
        - Events within the watermark are assigned to all overlapping sliding windows
          `[w_start, w_start + 60s)` where `w_start` is a multiple of `slide_seconds` (10s).
        Persists `streaming_window_metrics` and `streaming_category_metrics` to the Serving Layer.
        """
        df = pd.DataFrame(events)
        df["event_ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["event_ts"]).copy()

        watermark_sec = watermark_minutes * 60
        df["accepted_by_watermark"] = df["delay_seconds"] <= watermark_sec

        accepted_df = df[df["accepted_by_watermark"]].copy()
        dropped_df = df[~df["accepted_by_watermark"]].copy()

        min_ts = accepted_df["event_ts"].min().floor(f"{slide_seconds}s")
        max_ts = accepted_df["event_ts"].max().ceil(f"{slide_seconds}s")

        window_starts = pd.date_range(
            start=min_ts - timedelta(seconds=window_seconds - slide_seconds),
            end=max_ts,
            freq=f"{slide_seconds}s",
        )

        window_rows: List[Dict[str, Any]] = []
        cat_rows: List[Dict[str, Any]] = []

        for w_start in window_starts:
            w_end = w_start + timedelta(seconds=window_seconds)
            mask = (accepted_df["event_ts"] >= w_start) & (accepted_df["event_ts"] < w_end)
            win_events = accepted_df.loc[mask]
            if win_events.empty:
                continue

            # Count how many too-late events would have belonged to this window before watermark eviction
            drop_mask = (dropped_df["event_ts"] >= w_start) & (dropped_df["event_ts"] < w_end)
            dropped_in_win = int(drop_mask.sum())

            total_ev = len(win_events)
            active_users = int(win_events["user_id"].nunique())
            active_sessions = int(win_events["session_id"].nunique())
            views = int((win_events["event_type"] == "view").sum())
            carts = int((win_events["event_type"] == "cart").sum())
            rem_carts = int((win_events["event_type"] == "remove_from_cart").sum())
            purchases = int((win_events["event_type"] == "purchase").sum())
            gross_sales = round(
                float(win_events.loc[win_events["event_type"] == "purchase", "price"].sum()), 2
            )
            conv_rate = round(purchases * 100.0 / max(1, views), 2)
            late_accepted = int(win_events["is_late"].sum())

            window_rows.append(
                {
                    "window_start": w_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "window_end": w_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "total_events": total_ev,
                    "active_users": active_users,
                    "active_sessions": active_sessions,
                    "view_count": views,
                    "cart_count": carts,
                    "remove_cart_count": rem_carts,
                    "purchase_count": purchases,
                    "gross_sales": gross_sales,
                    "conversion_rate_pct": conv_rate,
                    "late_events_accepted": late_accepted,
                    "late_events_dropped": dropped_in_win,
                }
            )

            # Category + event_type breakdown for the most recent windows
            cat_grp = win_events.groupby(["category", "event_type"], as_index=False).agg(
                event_count=("event_id", "count"),
                total_value=("price", "sum"),
            )
            for _, crow in cat_grp.iterrows():
                cat_rows.append(
                    {
                        "window_start": w_start.strftime("%Y-%m-%d %H:%M:%S"),
                        "window_end": w_end.strftime("%Y-%m-%d %H:%M:%S"),
                        "category": crow["category"],
                        "event_type": crow["event_type"],
                        "event_count": int(crow["event_count"]),
                        "total_value": round(float(crow["total_value"]), 2),
                    }
                )

        win_df = pd.DataFrame(window_rows)
        cat_df = pd.DataFrame(cat_rows)

        if not win_df.empty:
            self.db_client.write_dataframe(win_df, "streaming_window_metrics", if_exists="replace")
        if not cat_df.empty:
            self.db_client.write_dataframe(cat_df, "streaming_category_metrics", if_exists="replace")

        logger.info(
            "Processed %d clickstream events into %d sliding windows (Watermark=%dm: %d late accepted, %d late dropped).",
            len(df),
            len(win_df),
            watermark_minutes,
            int(accepted_df["is_late"].sum()),
            len(dropped_df),
        )
        return win_df, cat_df

