"""
Real-Time Clickstream Event Generator for Apache Kafka (Core 1 - Ingestion Layer).
Reference:
- Proposal Section II.1: Streaming Data from Python Event Generator:
  (user_id, product_id, event_type, session_id, timestamp)
- Proposal Section III.1 & III.3: Publishes to Kafka topic `clickstream-events`.
  Includes controlled Late-Arriving Data (Out-of-Order timestamps) to test Spark
  Structured Streaming Watermarking (10 minutes) & State Store Cleanup.
- IT4931 Note 5 (Apache Kafka):
  - Uses Semantic Partitioning (`key = user_id`) so all events of the same user land in the
    same Topic Partition, preserving strict per-user sequential ordering.
  - Configures `acks='all'` (-1) for highest durability across In-Sync Replicas (ISRs).
"""

from __future__ import annotations

import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from src.config import KAFKA_CONFIG, LOCAL_DATA_DIR, KafkaConfig
from src.ingestion.historical_data_loader import CATEGORIES, PRICE_RANGES

logger = logging.getLogger(__name__)

EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]
EVENT_WEIGHTS = [0.68, 0.18, 0.05, 0.09]


class ClickstreamEventGenerator:
    """
    Generates realistic e-commerce clickstream events with session state and
    configurable late-data injection for Watermarking verification.
    """

    def __init__(
        self,
        config: KafkaConfig = KAFKA_CONFIG,
        num_users: int = 5000,
        num_products: int = 2000,
        seed: int = 42,
    ) -> None:
        self.config = config
        self.rng = random.Random(seed)
        self.num_users = num_users
        self.num_products = num_products
        self.active_sessions: Dict[str, str] = {}

    def generate_single_event(self, reference_time: datetime | None = None) -> Dict[str, Any]:
        """
        Generate a single clickstream event matching Proposal Section II.1:
        `user_id, product_id, event_type, session_id, timestamp` (plus `category`, `price`, `is_late`, `delay_seconds`).
        """
        now = reference_time or datetime.now(timezone.utc).replace(tzinfo=None)
        user_id = f"USER_{self.rng.randint(1, self.num_users):05d}"

        # Reuse session ID 80% of the time for the same user to model multi-step funnels
        if user_id not in self.active_sessions or self.rng.random() < 0.20:
            self.active_sessions[user_id] = f"SESS_{uuid.uuid4().hex[:12].upper()}"
        session_id = self.active_sessions[user_id]

        product_num = int(self.rng.paretovariate(1.2)) % self.num_products + 1
        product_id = f"PROD_{product_num:05d}"
        category = CATEGORIES[product_num % len(CATEGORIES)]
        low_p, high_p = PRICE_RANGES[category]
        price = round(self.rng.uniform(low_p, high_p), 2)

        event_type = self.rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

        # Simulate network lag / mobile offline caching (Veracity - Late Data)
        is_late = False
        delay_seconds = 0
        if self.rng.random() < self.config.late_data_probability:
            is_late = True
            # 75% of late data is within the 10-min watermark (1-9 mins), 25% exceeds watermark (11-18 mins)
            if self.rng.random() < 0.75:
                delay_seconds = self.rng.randint(60, 540)
            else:
                delay_seconds = self.rng.randint(660, max(720, self.config.max_late_minutes * 60))

        event_time = now - timedelta(seconds=delay_seconds)

        return {
            "event_id": f"EVT_{uuid.uuid4().hex[:14].upper()}",
            "user_id": user_id,
            "product_id": product_id,
            "event_type": event_type,
            "session_id": session_id,
            "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "category": category,
            "price": price,
            "is_late": is_late,
            "delay_seconds": delay_seconds,
        }

    def generate_batch(
        self,
        count: int = 1000,
        start_time: datetime | None = None,
        step_ms: int = 50,
    ) -> List[Dict[str, Any]]:
        """Generate a batch of chronological clickstream events."""
        current_time = start_time or (
            datetime.now(timezone.utc).replace(tzinfo=None)
            - timedelta(seconds=int(count * step_ms / 1000.0))
        )
        events: List[Dict[str, Any]] = []
        for _ in range(count):
            events.append(self.generate_single_event(reference_time=current_time))
            current_time += timedelta(milliseconds=step_ms)
        return events

    def save_jsonl_to_raw_layer(
        self,
        events: List[Dict[str, Any]],
        output_path: Path | None = None,
    ) -> Path:
        """Write semi-structured JSON lines to the Raw Data Layer (`data/raw/clickstream_json/`)."""
        target = output_path or (LOCAL_DATA_DIR / "raw" / "clickstream_json" / "clickstream_events.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev) + "\n")
        logger.info("Saved %d clickstream JSON events to %s", len(events), target)
        return target

    def stream_to_kafka(self, max_events: int | None = None) -> None:
        """
        Continuously publish clickstream events to Apache Kafka topic `clickstream-events`.
        Uses `user_id` as the Kafka message key for semantic partitioning (IT4931 Note 5).
        """
        from kafka import KafkaProducer

        logger.info(
            "Connecting to Kafka Broker at %s (Topic: %s)...",
            self.config.bootstrap_servers,
            self.config.topic,
        )
        producer = KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers.split(","),
            acks="all",  # request.required.acks = -1 (Highest durability across ISRs)
            retries=5,
            linger_ms=20,  # Micro-batching to amortize network I/O overhead (Note 5 Slide 39)
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        interval = 1.0 / max(1.0, self.config.events_per_second)
        sent = 0
        try:
            while max_events is None or sent < max_events:
                event = self.generate_single_event()
                # Semantic Partitioning: key = user_id ensures ordered events per user in a partition
                producer.send(
                    self.config.topic,
                    key=event["user_id"],
                    value=event,
                )
                sent += 1
                if sent % 250 == 0:
                    producer.flush()
                    logger.info(
                        "Published %d clickstream events to Kafka topic '%s' (latest event_type=%s, is_late=%s)",
                        sent,
                        self.config.topic,
                        event["event_type"],
                        event["is_late"],
                    )
                time.sleep(interval)
        finally:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    generator = ClickstreamEventGenerator()
    while True:
        try:
            generator.stream_to_kafka()
        except Exception as err:
            logger.warning("Kafka connection retry in 5s (%s)...", err)
            time.sleep(5)

