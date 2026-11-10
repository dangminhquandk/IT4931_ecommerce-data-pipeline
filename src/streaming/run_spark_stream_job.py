"""Entrypoint for submitting Spark Structured Streaming job via spark-submit."""

import logging
from src.streaming.stream_processor import ClickstreamStreamProcessor

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    processor = ClickstreamStreamProcessor()
    processor.start_spark_structured_streaming()

