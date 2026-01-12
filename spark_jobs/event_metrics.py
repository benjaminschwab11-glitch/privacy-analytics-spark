"""
Spark Event Metrics Job
Calculates daily event aggregations by type, region, and device
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, date_trunc, to_date, current_timestamp, udf
)
from pyspark.sql.types import StringType
import time

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_config import DatabaseConfig


class EventMetricsCalculator:
    """Calculate event metrics with Spark"""
    
    def __init__(self):
        """Initialize Spark session"""
        
        self.spark = SparkSession.builder \
            .appName("EventMetricsCalculation") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.jars", str(Path(__file__).parent.parent / "jars" / "postgresql-42.7.1.jar")) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"Spark session created: {self.spark.version}")
    
    def register_tokenize_udf(self):
        """Register UDF to tokenize user_id"""
        import hashlib
        import hmac
        
        secret_key = os.getenv('SECRET_KEY', 'spark-secret')
        
        @udf(StringType())
        def tokenize_user_id(value):
            if value:
                return 'tok_' + hmac.new(
                    secret_key.encode('utf-8'),
                    str(value).encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()[:20]
            return None
        
        return tokenize_user_id

    def read_anonymized_users(self):
        """Read anonymized users for region mapping"""
        
        print("\nReading anonymized users...")
        
        jdbc_url = DatabaseConfig.get_jdbc_url()
        properties = DatabaseConfig.get_connection_properties()
        
        users = self.spark.read \
            .jdbc(
                url=jdbc_url,
                table='anonymized_users',
                properties=properties
            ) \
            .select('user_token', 'region')
        
        print(f"✓ Loaded {users.count():,} users")
        
        return users
    
    def read_events_from_file(self, num_events=100000):
        """
        Read events from JSON file for demonstration
        
        Args:
            num_events: Number of events to load
        """
        
        events_file = str(Path(__file__).parent.parent / 'data' / 'events.jsonl')
        
        print(f"\nReading events from: {events_file}")
        print(f"Loading first {num_events:,} events...")
        
        # Read events
        df = self.spark.read.json(events_file).limit(num_events)
        
        print(f"✓ Loaded {df.count():,} events")
        
        return df
    
    def calculate_daily_metrics(self, events, users):
        """
        Calculate daily event metrics
        
        Args:
            events: Events DataFrame
            users: Users DataFrame with regions
            
        Returns:
            Daily metrics DataFrame
        """
        
        print("\nCalculating daily event metrics...")
        start_time = time.time()
        
        # Tokenize event user_ids to match anonymized users
        tokenize = self.register_tokenize_udf()
        
        events_tokenized = events.withColumn(
            'user_token',
            tokenize(col('user_id'))
        )
        
        # Join events with users to get region
        events_with_region = events_tokenized.join(
            users,
            'user_token',
            'left'
        )
        
        # Extract date from timestamp
        events_with_date = events_with_region.withColumn(
            'event_date',
            to_date(col('timestamp'))
        )
        
        # Calculate metrics
        daily_metrics = events_with_date.groupBy(
            'event_date',
            'event_type',
            'region',
            'device_type'
        ).agg(
            count('*').alias('total_events'),
            countDistinct('user_id').alias('unique_users'),
            avg('duration_seconds').alias('avg_session_duration')
        ).orderBy('event_date', col('total_events').desc())
        
        # Cast avg_session_duration
        daily_metrics = daily_metrics.withColumn(
            'avg_session_duration',
            col('avg_session_duration').cast('integer')
        )
        
        # Add timestamp
        daily_metrics = daily_metrics.withColumn('created_at', current_timestamp())
        
        metrics_count = daily_metrics.count()
        elapsed = time.time() - start_time
        
        print(f"✓ Calculated {metrics_count:,} daily metrics in {elapsed:.2f} seconds")
        
        return daily_metrics
    
    def show_metrics_insights(self, metrics):
        """Display metrics insights"""
        
        print("\nEvent Metrics Insights:")
        print("=" * 80)
        
        # Top events by volume
        print("\nTop 20 Event Combinations by Volume:")
        metrics.orderBy(col('total_events').desc()) \
            .select(
                'event_date',
                'event_type',
                'region',
                'device_type',
                'total_events',
                'unique_users'
            ).show(20, truncate=False)
        
        # Events by type
        print("\nEvents by Type:")
        metrics.groupBy('event_type').agg(
            count('*').alias('metric_count'),
            avg('total_events').alias('avg_events_per_day'),
            avg('unique_users').alias('avg_unique_users')
        ).orderBy(col('avg_events_per_day').desc()).show()
        
        # Events by region
        print("\nEvents by Region:")
        metrics.groupBy('region').agg(
            count('*').alias('metric_count'),
            avg('total_events').alias('avg_events'),
            avg('unique_users').alias('avg_users')
        ).orderBy(col('avg_events').desc()).show()
        
        # Events by device
        print("\nEvents by Device Type:")
        metrics.groupBy('device_type').agg(
            count('*').alias('metric_count'),
            avg('total_events').alias('avg_events'),
            avg('unique_users').alias('avg_users')
        ).orderBy(col('avg_events').desc()).show()
    
    def write_metrics_to_postgres(self, metrics):
        """Write metrics to PostgreSQL"""
        
        print("\nWriting daily metrics to PostgreSQL...")
        start_time = time.time()
        
        jdbc_url = DatabaseConfig.get_jdbc_url()
        properties = DatabaseConfig.get_connection_properties()
        
        # Write to PostgreSQL
        metrics.write \
            .jdbc(
                url=jdbc_url,
                table='daily_event_metrics',
                mode='overwrite',
                properties=properties
            )
        
        elapsed = time.time() - start_time
        record_count = metrics.count()
        
        print(f"✓ Written {record_count:,} metrics to PostgreSQL in {elapsed:.2f} seconds")
    
    def run(self, num_events=100000):
        """Execute event metrics pipeline"""
        
        print("\n" + "=" * 80)
        print("SPARK JOB: Event Metrics Calculation")
        print("=" * 80)
        
        total_start = time.time()
        
        try:
            # Read users (for region mapping)
            users = self.read_anonymized_users()
            
            # Read events
            events = self.read_events_from_file(num_events)
            
            # Calculate metrics
            daily_metrics = self.calculate_daily_metrics(events, users)
            
            # Show insights
            self.show_metrics_insights(daily_metrics)
            
            # Write to database
            self.write_metrics_to_postgres(daily_metrics)
            
            total_elapsed = time.time() - total_start
            
            print("\n" + "=" * 80)
            print(f"✓ Event metrics calculation completed in {total_elapsed:.2f} seconds")
            print("=" * 80)
            
        except Exception as e:
            print(f"\n✗ Job failed: {e}")
            raise
        
        finally:
            self.spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate event metrics')
    parser.add_argument('--events', type=int, default=100000, 
                       help='Number of events to process (default: 100,000)')
    
    args = parser.parse_args()
    
    calculator = EventMetricsCalculator()
    calculator.run(num_events=args.events)

