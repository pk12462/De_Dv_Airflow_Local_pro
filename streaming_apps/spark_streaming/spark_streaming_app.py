"""
Spark Structured Streaming Application
=======================================
Concrete Spark Streaming app:
  - Source: Kafka topic (JSON messages)
  - Process: Schema enforcement, watermarking, deduplication
  - Sink: Delta Lake / Parquet / Kafka
"""
from __future__ import annotations

import argparse
import logging
import os
from typing import Any

from streaming_apps.spark_streaming.base_streaming_app import BaseSparkStreamingApp
from streaming_apps.spark_streaming.transformations.stream_transforms import StreamTransforms

logger = logging.getLogger(__name__)


class SparkStreamingApp(BaseSparkStreamingApp):
    """
    Kafka → Spark Structured Streaming → Delta Lake pipeline.

    Config keys expected::

        spark:
          master: local[*]
          checkpoint_dir: /tmp/spark/checkpoints
          trigger_interval: 10 seconds
        kafka:
          bootstrap_servers: localhost:9092
          source_topic: raw.events
          sink_topic: processed.events
        sink:
          format: delta         # delta | parquet | kafka | console
          path: /data/output    # for delta/parquet sinks
    """

    def __init__(self, config: dict[str, Any]) -> None:
        app_name = config.get("spark", {}).get("app_name", "de-dv-spark-streaming")
        super().__init__(app_name, config)
        self._spark_cfg = config.get("spark", {})
        self._kafka_cfg = config.get("kafka", {})
        self._sink_cfg = config.get("sink", {})

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def build_session(self) -> Any:
        """Create a SparkSession with Kafka and Delta Lake packages."""
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise ImportError("pyspark is not installed. Run: pip install pyspark") from exc

        master = self._spark_cfg.get("master", os.getenv("SPARK_MASTER_URL", "local[*]"))
        spark = (
            SparkSession.builder.appName(self.app_name)
            .master(master)
            .config("spark.sql.shuffle.partitions", self._spark_cfg.get("shuffle_partitions", "4"))
            .config(
                "spark.sql.streaming.checkpointLocation",
                self._spark_cfg.get("checkpoint_dir", os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark/checkpoints")),
            )
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(self._spark_cfg.get("log_level", "WARN"))
        logger.info("SparkSession created: master=%s appName=%s", master, self.app_name)
        return spark

    def read_stream(self) -> Any:
        """Read a streaming DataFrame from Kafka."""
        bootstrap_servers = self._kafka_cfg.get(
            "bootstrap_servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        )
        source_topic = self._kafka_cfg.get(
            "source_topic", os.getenv("KAFKA_SOURCE_TOPIC", "raw.events")
        )
        logger.info("Reading stream from topic=%s brokers=%s", source_topic, bootstrap_servers)
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", source_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def process(self, stream_df: Any) -> Any:
        """Parse JSON payload and apply streaming transformations."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            MapType, StringType, StructField, StructType, TimestampType,
        )

        event_schema = StructType([
            StructField("event_id", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
            StructField("timestamp_ms", StringType(), nullable=True),
            StructField("payload", MapType(StringType(), StringType()), nullable=True),
        ])

        parsed = (
            stream_df
            .select(
                F.col("key").cast("string").alias("kafka_key"),
                F.from_json(F.col("value").cast("string"), event_schema).alias("data"),
                F.col("timestamp").alias("kafka_ts"),
            )
            .select("kafka_key", "kafka_ts", "data.*")
            .withColumn("event_ts", (F.col("timestamp_ms").cast("long") / 1000).cast(TimestampType()))
        )

        transforms = StreamTransforms(self._spark_cfg)
        deduplicated = transforms.deduplicate(parsed, id_col="event_id", watermark_col="event_ts")
        return deduplicated

    def write_stream(self, processed_df: Any) -> Any:
        """Write the processed stream to the configured sink."""
        sink_format = self._sink_cfg.get("format", "console")
        trigger_interval = self._spark_cfg.get("trigger_interval", "10 seconds")

        from pyspark.sql.streaming import DataStreamWriter

        writer: DataStreamWriter = (
            processed_df.writeStream
            .trigger(processingTime=trigger_interval)
            .option(
                "checkpointLocation",
                self._spark_cfg.get("checkpoint_dir", "/tmp/spark/checkpoints"),
            )
        )

        if sink_format == "console":
            query = writer.format("console").outputMode("append").start()
        elif sink_format in ("delta", "parquet"):
            path = self._sink_cfg.get("path", f"/tmp/spark/output/{sink_format}")
            query = writer.format(sink_format).outputMode("append").option("path", path).start()
        elif sink_format == "kafka":
            sink_topic = self._kafka_cfg.get("sink_topic", os.getenv("KAFKA_SINK_TOPIC", "processed.events"))
            bootstrap_servers = self._kafka_cfg.get("bootstrap_servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
            from pyspark.sql import functions as F
            query = (
                writer
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("topic", sink_topic)
                .outputMode("append")
                .start()
            )
        else:
            raise ValueError(f"Unsupported sink format: {sink_format}")

        logger.info("Streaming query started with sink=%s trigger=%s", sink_format, trigger_interval)
        return query


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import yaml

    parser = argparse.ArgumentParser(description="Spark Structured Streaming App")
    parser.add_argument(
        "--config",
        default="pipelines/configs/streaming_pipeline_config.yaml",
        help="Path to pipeline YAML config",
    )
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    app = SparkStreamingApp(cfg)
    try:
        app.run()
    except KeyboardInterrupt:
        app.stop()

