"""
Centralized configuration for the Real-Time E-Commerce Data Pipeline (IT4931 - Group 3).
Aligns with:
- Proposal Section III (4 Core Layers: HDFS/Kafka, Spark Batch, Spark Streaming, PostgreSQL/Streamlit)
- Proposal Section IV (SQL Benchmark: Raw CSV vs Unpartitioned Parquet vs Partitioned Parquet)
- IT4931 Lectures (Note 3: HDFS & Parquet, Note 5: Kafka, Note 8: Lambda Architecture)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_DATA_DIR = BASE_DIR / "data"


@dataclass(frozen=True)
class HDFSConfig:
    """
    HDFS Storage Layer Configuration (Note 3 - HDFS & Parquet).
    - Block size: 128MB (134,217,728 bytes)
    - Replication factor: 1 (Docker single-cluster environment, expandable to 3 in production)
    - 3 Storage Tiers:
      1. Raw Data Layer (CSV / JSON - Immutable Master Dataset, WORM semantics)
      2. Refined Data Layer (Columnar Parquet + Snappy compression, unpartitioned)
      3. Partitioned Data Layer (Parquet + Snappy, partitioned by year=/month=/ and event_type=)
    """

    namenode_uri: str = os.getenv("HDFS_NAMENODE_URI", "hdfs://hdfs-namenode-service:9000")
    use_local_fs: bool = os.getenv("USE_LOCAL_FS", "false").lower() in ("1", "true", "yes")
    block_size_bytes: int = int(os.getenv("HDFS_BLOCK_SIZE", str(128 * 1024 * 1024)))  # 128 MB
    replication_factor: int = int(os.getenv("HDFS_REPLICATION", "1"))

    # Parquet tuning parameters (Note 3 Slide 43: parquet.block.size, parquet.page.size, etc.)
    parquet_compression: str = os.getenv("PARQUET_COMPRESSION", "snappy")
    parquet_row_group_size_bytes: int = int(
        os.getenv("PARQUET_ROW_GROUP_SIZE", str(128 * 1024 * 1024))
    )
    parquet_page_size_bytes: int = int(os.getenv("PARQUET_PAGE_SIZE", str(1 * 1024 * 1024)))
    parquet_dictionary_page_size_bytes: int = int(
        os.getenv("PARQUET_DICT_PAGE_SIZE", str(1 * 1024 * 1024))
    )
    parquet_enable_dictionary: bool = True

    @property
    def root_uri(self) -> str:
        if self.use_local_fs:
            return str(LOCAL_DATA_DIR.as_posix())
        return f"{self.namenode_uri}/data"

    @property
    def raw_transactions_csv_path(self) -> str:
        return f"{self.root_uri}/raw/transactions_csv"

    @property
    def raw_products_csv_path(self) -> str:
        return f"{self.root_uri}/raw/products_csv"

    @property
    def raw_clickstream_json_path(self) -> str:
        return f"{self.root_uri}/raw/clickstream_json"

    @property
    def refined_transactions_unpartitioned_path(self) -> str:
        return f"{self.root_uri}/refined/transactions_parquet_unpartitioned"

    @property
    def refined_products_parquet_path(self) -> str:
        return f"{self.root_uri}/refined/products_parquet"

    @property
    def partitioned_transactions_path(self) -> str:
        return f"{self.root_uri}/partitioned/transactions_parquet_partitioned"

    @property
    def partitioned_clickstream_path(self) -> str:
        return f"{self.root_uri}/partitioned/clickstream_parquet_partitioned"

    @property
    def streaming_checkpoint_path(self) -> str:
        return f"{self.root_uri}/checkpoints/clickstream_streaming"


@dataclass(frozen=True)
class KafkaConfig:
    """
    Apache Kafka Ingestion Layer Configuration (Note 5 - Kafka).
    - Topic: clickstream-events
    - Semantic partitioning by user_id to preserve per-user event ordering within each partition.
    - Required acks = -1 ('all') for highest durability across ISRs.
    """

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-service:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "clickstream-events")
    num_partitions: int = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))
    consumer_group_id: str = os.getenv("KAFKA_CONSUMER_GROUP", "spark-structured-streaming-cg")
    events_per_second: float = float(os.getenv("EVENTS_PER_SECOND", "25.0"))
    late_data_probability: float = float(os.getenv("LATE_DATA_PROBABILITY", "0.06"))
    max_late_minutes: int = int(os.getenv("MAX_LATE_MINUTES", "15"))


@dataclass(frozen=True)
class StreamingConfig:
    """
    Spark Structured Streaming Configuration (Proposal Section III.3).
    - Event-Time Processing
    - Sliding Window: 1 minute duration, sliding every 10 seconds
    - Watermarking: 10 minutes threshold to handle Late Data and clean up State Store
    """

    window_duration: str = os.getenv("STREAM_WINDOW_DURATION", "1 minute")
    slide_duration: str = os.getenv("STREAM_SLIDE_DURATION", "10 seconds")
    watermark_delay: str = os.getenv("STREAM_WATERMARK_DELAY", "10 minutes")
    trigger_interval: str = os.getenv("STREAM_TRIGGER_INTERVAL", "10 seconds")


@dataclass(frozen=True)
class DatabaseConfig:
    """
    Serving Layer Database Configuration (Proposal Section III.4).
    Supports PostgreSQL in Docker cluster and automatic SQLite fallback for local standalone mode.
    """

    host: str = os.getenv("POSTGRES_HOST", "postgresql-service")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "ecommerce_serving")
    user: str = os.getenv("POSTGRES_USER", "postgres")
    password: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    sqlite_fallback_path: Path = LOCAL_DATA_DIR / "serving_layer.db"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def sqlalchemy_postgres_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def sqlalchemy_sqlite_url(self) -> str:
        self.sqlite_fallback_path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{self.sqlite_fallback_path.as_posix()}"


HDFS_CONFIG = HDFSConfig()
KAFKA_CONFIG = KafkaConfig()
STREAMING_CONFIG = StreamingConfig()
DB_CONFIG = DatabaseConfig()

