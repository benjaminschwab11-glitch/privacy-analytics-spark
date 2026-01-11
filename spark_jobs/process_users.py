"""
Spark Batch Job - Process Users
Reads raw user data, applies anonymization, loads to PostgreSQL
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, udf, when, lit, to_timestamp
import time

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from privacy.anonymizer import Anonymizer
from database.db_config import DatabaseConfig


class UserProcessor:
    """Spark job to process and anonymize users"""
    
    def __init__(self, batch_size=100000):
        """
        Initialize processor
        
        Args:
            batch_size: Number of records to process
        """
        self.batch_size = batch_size
        self.anonymizer = Anonymizer(secret_key=os.getenv('SECRET_KEY', 'spark-secret'))
        self.data_dir = Path(__file__).parent.parent / 'data'
        
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("UserAnonymizationBatch") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.jars", str(Path(__file__).parent.parent / "jars" / "postgresql-42.7.1.jar")) \
            .getOrCreate()
        
        # Reduce logging noise
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"Spark session created: {self.spark.version}")
        print(f"Available cores: {self.spark.sparkContext.defaultParallelism}")
    
    def register_udfs(self):
        """Register User Defined Functions for anonymization"""
        
        # Get secret key once
        secret_key = os.getenv('SECRET_KEY', 'spark-secret')
        
        # Hash UDF - independent function
        @udf(StringType())
        def hash_value(value):
            if value:
                import hashlib
                import hmac
                return hmac.new(
                    secret_key.encode('utf-8'),
                    str(value).encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
            return None
        
        # Tokenize UDF - simplified (just hash for now, tokens need state)
        @udf(StringType())
        def tokenize_user_id(value):
            if value:
                import hashlib
                import hmac
                # Use hash as token (not reversible, but works for batch processing)
                return 'tok_' + hmac.new(
                    secret_key.encode('utf-8'),
                    str(value).encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()[:20]
            return None
        
        # Generalize age UDF - pure function
        @udf(StringType())
        def generalize_age(age):
            if age:
                age_int = int(age)
                if age_int < 18:
                    return "0-17"
                elif age_int < 25:
                    return "18-24"
                elif age_int < 35:
                    return "25-34"
                elif age_int < 45:
                    return "35-44"
                elif age_int < 55:
                    return "45-54"
                elif age_int < 65:
                    return "55-64"
                else:
                    return "65+"
            return None
        
        # Generalize city UDF - pure function
        @udf(StringType())
        def generalize_city(city):
            if city:
                regions = {
                    'New York': 'Northeast',
                    'Philadelphia': 'Northeast',
                    'Boston': 'Northeast',
                    'Los Angeles': 'West',
                    'San Diego': 'West',
                    'San Francisco': 'West',
                    'San Jose': 'West',
                    'Seattle': 'West',
                    'Portland': 'West',
                    'Phoenix': 'Southwest',
                    'Dallas': 'Southwest',
                    'Houston': 'Southwest',
                    'Austin': 'Southwest',
                    'San Antonio': 'Southwest',
                    'Chicago': 'Midwest',
                    'Denver': 'Midwest'
                }
                return regions.get(str(city), 'Other')
            return None
        
        # Generalize ZIP UDF - pure function
        @udf(StringType())
        def generalize_zip(zip_code):
            if zip_code:
                digits = ''.join(c for c in str(zip_code) if c.isdigit())
                if len(digits) >= 3:
                    return digits[:3] + '**'
                else:
                    return '***'
            return None
        
        # Mask phone UDF - pure function
        @udf(StringType())
        def mask_phone(phone):
            if phone:
                digits = ''.join(c for c in str(phone) if c.isdigit())
                if len(digits) >= 4:
                    return '***-***-' + digits[-4:]
                else:
                    return '***'
            return None
        
        # Mask IP UDF - pure function
        @udf(StringType())
        def mask_ip(ip):
            if ip and '.' in str(ip):
                parts = str(ip).split('.')
                if len(parts) == 4:
                    parts[-1] = '0'
                    return '.'.join(parts)
            return str(ip) if ip else None
        
        return {
            'hash_value': hash_value,
            'tokenize_user_id': tokenize_user_id,
            'generalize_age': generalize_age,
            'generalize_city': generalize_city,
            'generalize_zip': generalize_zip,
            'mask_phone': mask_phone,
            'mask_ip': mask_ip
        }
    
    def read_users(self):
        """Read user data from JSON Lines file"""
        users_file = str(self.data_dir / 'users.jsonl')
        
        print(f"\nReading users from: {users_file}")
        
        # Read JSON Lines with Spark
        df = self.spark.read.json(users_file)
        
        # Limit to batch size
        if self.batch_size:
            df = df.limit(self.batch_size)
        
        print(f"✓ Loaded {df.count():,} users")
        
        return df
    
    def anonymize_users(self, df):
        """Apply anonymization transformations"""
        
        print("\nApplying anonymization transformations...")
        start_time = time.time()
        
        # Register UDFs
        udfs = self.register_udfs()
        
        # Apply transformations
        anonymized = df.select(
            udfs['tokenize_user_id'](col('user_id')).alias('user_token'),
            udfs['hash_value'](col('email')).alias('email_hash'),
            udfs['hash_value'](col('first_name')).alias('first_name_hash'),
            udfs['hash_value'](col('last_name')).alias('last_name_hash'),
            udfs['generalize_age'](col('age')).alias('age_bucket'),
            col('gender'),
            udfs['generalize_city'](col('city')).alias('region'),
            col('state'),
            udfs['generalize_zip'](col('zip_code')).alias('zip_prefix'),
            udfs['mask_ip'](col('ip_address')).alias('ip_subnet'),
            udfs['mask_phone'](col('phone')).alias('phone_suffix'),
            to_timestamp(col('created_at')).alias('created_at'),
            to_timestamp(col('last_login')).alias('last_login'),
            col('account_status'),
            col('subscription_tier'),
            col('lifetime_value').cast('decimal(10,2)'),
            col('total_purchases').cast('integer')
        ) 

        # Cache for reuse
        anonymized.cache()
        
        record_count = anonymized.count()
        elapsed = time.time() - start_time
        
        print(f"✓ Anonymized {record_count:,} records in {elapsed:.2f} seconds")
        print(f"  Rate: {record_count/elapsed:,.0f} records/second")
        
        return anonymized
    
    def show_sample(self, df, num_rows=5):
        """Show sample anonymized data"""
        print(f"\nSample Anonymized Users ({num_rows} records):")
        print("=" * 80)
        
        df.select(
            'user_token',
            'age_bucket',
            'gender',
            'region',
            'subscription_tier',
            'lifetime_value'
        ).show(num_rows, truncate=False)
    
    def write_to_postgres(self, df):
        """Write anonymized users to PostgreSQL"""
        
        print("\nWriting to PostgreSQL...")
        start_time = time.time()
        
        jdbc_url = DatabaseConfig.get_jdbc_url()
        properties = DatabaseConfig.get_connection_properties()
        
        # Write to PostgreSQL
        df.write \
            .jdbc(
                url=jdbc_url,
                table='anonymized_users',
                mode='append',
                properties=properties
            )
        
        elapsed = time.time() - start_time
        
        print(f"✓ Written to PostgreSQL in {elapsed:.2f} seconds")
    
    def get_statistics(self, df):
        """Calculate and display statistics"""
        
        print("\nDataset Statistics:")
        print("=" * 80)
        
        # Region distribution
        print("\nUsers by Region:")
        df.groupBy('region').count().orderBy(col('count').desc()).show()
        
        # Age distribution
        print("Users by Age Bucket:")
        df.groupBy('age_bucket').count().orderBy('age_bucket').show()
        
        # Subscription tier
        print("Users by Subscription Tier:")
        df.groupBy('subscription_tier').count().orderBy(col('count').desc()).show()
        
        # Summary stats
        df.select('lifetime_value', 'total_purchases').summary().show()
    
    def run(self):
        """Execute the complete pipeline"""
        
        print("\n" + "=" * 80)
        print("SPARK BATCH JOB: User Anonymization")
        print("=" * 80)
        
        total_start = time.time()
        
        try:
            # Read data
            raw_users = self.read_users()
            
            # Anonymize
            anonymized_users = self.anonymize_users(raw_users)
            
            # Show sample
            self.show_sample(anonymized_users)
            
            # Statistics
            self.get_statistics(anonymized_users)
            
            # Write to database
            self.write_to_postgres(anonymized_users)
            
            total_elapsed = time.time() - total_start
            
            print("\n" + "=" * 80)
            print(f"✓ Job completed successfully in {total_elapsed:.2f} seconds")
            print("=" * 80)
            
        except Exception as e:
            print(f"\n✗ Job failed: {e}")
            raise
        
        finally:
            self.spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process and anonymize users with Spark')
    parser.add_argument('--batch-size', type=int, default=100000, 
                       help='Number of users to process (default: 100,000)')
    
    args = parser.parse_args()
    
    processor = UserProcessor(batch_size=args.batch_size)
    processor.run()

