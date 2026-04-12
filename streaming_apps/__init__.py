"""
Streaming Apps Package
======================
Provides Kafka (consumer/producer) and Spark Structured Streaming applications.
"""
from streaming_apps.kafka.consumer.kafka_consumer import KafkaConsumerApp
from streaming_apps.kafka.producer.kafka_producer import KafkaProducerApp
from streaming_apps.spark_streaming.spark_streaming_app import SparkStreamingApp

__all__ = ["KafkaConsumerApp", "KafkaProducerApp", "SparkStreamingApp"]
__version__ = "1.0.0"

