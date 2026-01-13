"""
Spark Streaming Consumer
Consumes events from Kafka and processes in real-time
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, countDistinct, 
    to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)

sys.path.insert(0, str(Path(__file__).parent.parent))


class StreamingConsumer:
    """Real-time event processing with Spark Streaming"""
    
    def __init__(self):
        """Initialize Spark streaming session"""
        
        jars_dir = Path(__file__).parent.parent / "jars"
        
        # Create Spark session with Kafka support
        self.spark = SparkSession.builder \
            .appName("KafkaEventStreaming") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.jars", ",".join([
                str(jars_dir / "spark-sql-kafka-0-10_2.13-3.5.0.jar"),
                str(jars_dir / "spark-token-provider-kafka-0-10_2.13-3.5.0.jar"),
                str(jars_dir / "kafka-clients-3.5.1.jar"),
                str(jars_dir / "commons-pool2-2.11.1.jar"),
                str(jars_dir / "postgresql-42.7.1.jar")
            ])) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"Spark Streaming session created: {self.spark.version}")
    
    def get_event_schema(self):
        """Define schema for incoming events"""
        
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("browser", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("search_query", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("video_id", StringType(), True),
            StructField("duration_seconds", IntegerType(), True)
        ])
    
    def read_from_kafka(self, topic='user-events'):
        """
        Read streaming data from Kafka
        
        Args:
            topic: Kafka topic to consume from
            
        Returns:
            Streaming DataFrame
        """
        print(f"\nConnecting to Kafka topic: {topic}")
        
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        print("✓ Connected to Kafka")
        
        # Parse JSON from Kafka value
        schema = self.get_event_schema()
        
        events = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp
        events = events.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        return events
    
    def process_windowed_aggregates(self, events):
        """
        Calculate windowed aggregations
        
        Args:
            events: Streaming DataFrame of events
            
        Returns:
            Aggregated streaming DataFrame
        """
        print("\nCalculating 1-minute windowed aggregations...")
        
        # 1-minute tumbling windows
        windowed_counts = events \
            .withWatermark("timestamp", "10 seconds") \
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("event_type"),
                col("device_type")
            ).agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users")
            )
        
        return windowed_counts
    
    def write_to_console(self, df, output_mode="update"):
        """
        Write streaming results to console
        
        Args:
            df: Streaming DataFrame
            output_mode: Output mode (append, update, complete)
        """
        query = df \
            .writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", False) \
            .start()
        
        return query
    
    def run_streaming_demo(self, duration_seconds=60):
        """
        Run streaming demo
        
        Args:
            duration_seconds: How long to run (seconds)
        """
        print("\n" + "=" * 80)
        print("SPARK STREAMING: Real-Time Event Processing")
        print("=" * 80)
        
        try:
            # Read from Kafka
            events = self.read_from_kafka()
            
            # Show raw events
            print("\n--- Streaming Raw Events ---")
            raw_query = events.select(
                "event_id",
                "event_type",
                "user_id",
                "device_type",
                "timestamp"
            ).writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .start()
            
            # Calculate windowed aggregations
            windowed = self.process_windowed_aggregates(events)
            
            # Show aggregations
            print("\n--- Windowed Aggregations (1-minute windows) ---")
            agg_query = self.write_to_console(windowed, output_mode="complete")
            
            # Wait
            print(f"\n✓ Streaming started. Running for {duration_seconds} seconds...")
            print("  (Press Ctrl+C to stop early)\n")
            
            import time
            time.sleep(duration_seconds)
            
            # Stop queries
            print("\nStopping streaming queries...")
            raw_query.stop()
            agg_query.stop()
            
            print("✓ Streaming stopped")
            
        except KeyboardInterrupt:
            print("\n\nStopped by user")
        
        except Exception as e:
            print(f"\n✗ Streaming failed: {e}")
            raise
        
        finally:
            self.spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Spark streaming consumer')
    parser.add_argument('--duration', type=int, default=60,
                       help='How long to run in seconds (default: 60)')
    
    args = parser.parse_args()
    
    consumer = StreamingConsumer()
    consumer.run_streaming_demo(duration_seconds=args.duration)

