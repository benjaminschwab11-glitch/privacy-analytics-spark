"""
Spark Cohort Analysis Job
Calculates user cohorts by month, region, age, and subscription
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, min as spark_min, 
    max as spark_max, date_trunc, to_date, current_timestamp
)
import time

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_config import DatabaseConfig


class CohortAnalyzer:
    """Analyze user cohorts with Spark"""
    
    def __init__(self):
        """Initialize Spark session"""
        
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("UserCohortAnalysis") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.jars", str(Path(__file__).parent.parent / "jars" / "postgresql-42.7.1.jar")) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"Spark session created: {self.spark.version}")
    
    def read_users_from_postgres(self):
        """Read anonymized users from PostgreSQL"""
        
        print("\nReading anonymized users from PostgreSQL...")
        
        jdbc_url = DatabaseConfig.get_jdbc_url()
        properties = DatabaseConfig.get_connection_properties()
        
        df = self.spark.read \
            .jdbc(
                url=jdbc_url,
                table='anonymized_users',
                properties=properties
            )
        
        print(f"✓ Loaded {df.count():,} users from database")
        
        return df
    
    def calculate_cohorts(self, df):
        """
        Calculate user cohorts by month, region, age, gender, and subscription
        
        Args:
            df: DataFrame of anonymized users
            
        Returns:
            Cohort aggregations
        """
        
        print("\nCalculating user cohorts...")
        start_time = time.time()
        
        # Extract cohort month from created_at
        df_with_cohort = df.withColumn(
            'cohort_month',
            date_trunc('month', col('created_at'))
        )
        
        # Calculate cohorts
        cohorts = df_with_cohort.groupBy(
            'cohort_month',
            'region',
            'age_bucket',
            'gender',
            'subscription_tier'
        ).agg(
            count('*').alias('total_users'),
            spark_sum(
                (col('account_status') == 'active').cast('int')
            ).alias('active_users'),
            avg('lifetime_value').alias('avg_lifetime_value'),
            avg('total_purchases').alias('avg_purchases'),
            spark_sum('lifetime_value').alias('total_revenue'),
            spark_min('created_at').alias('first_user'),
            spark_max('created_at').alias('last_user')
        ).orderBy('cohort_month', 'region', 'total_users')
        
        # Cast decimals properly
        cohorts = cohorts.withColumn(
            'avg_lifetime_value',
            col('avg_lifetime_value').cast('decimal(10,2)')
        ).withColumn(
            'avg_purchases',
            col('avg_purchases').cast('decimal(10,2)')
        ).withColumn(
            'total_revenue',
            col('total_revenue').cast('decimal(12,2)')
        )
        
        # Add timestamp
        cohorts = cohorts.withColumn('created_at', current_timestamp())
        
        cohort_count = cohorts.count()
        elapsed = time.time() - start_time
        
        print(f"✓ Calculated {cohort_count:,} cohorts in {elapsed:.2f} seconds")
        
        return cohorts
    
    def show_cohort_insights(self, cohorts):
        """Display cohort insights"""
        
        print("\nCohort Insights:")
        print("=" * 80)
        
        # Top cohorts by revenue
        print("\nTop 10 Cohorts by Total Revenue:")
        cohorts.orderBy(col('total_revenue').desc()) \
            .select(
                'cohort_month',
                'region',
                'age_bucket',
                'subscription_tier',
                'total_users',
                'total_revenue'
            ).show(10, truncate=False)
        
        # Cohort summary by region
        print("\nCohort Summary by Region:")
        cohorts.groupBy('region').agg(
            spark_sum('total_users').alias('total_users'),
            avg('avg_lifetime_value').alias('avg_ltv'),
            spark_sum('total_revenue').alias('total_revenue')
        ).orderBy(col('total_revenue').desc()).show()
        
        # Cohort summary by age bucket
        print("\nCohort Summary by Age Bucket:")
        cohorts.groupBy('age_bucket').agg(
            spark_sum('total_users').alias('total_users'),
            avg('avg_lifetime_value').alias('avg_ltv'),
            spark_sum('total_revenue').alias('total_revenue')
        ).orderBy('age_bucket').show()
        
        # Cohort summary by subscription tier
        print("\nCohort Summary by Subscription Tier:")
        cohorts.groupBy('subscription_tier').agg(
            spark_sum('total_users').alias('total_users'),
            avg('avg_lifetime_value').alias('avg_ltv'),
            avg('avg_purchases').alias('avg_purchases'),
            spark_sum('total_revenue').alias('total_revenue')
        ).orderBy(col('total_revenue').desc()).show()
    
    def write_cohorts_to_postgres(self, cohorts):
        """Write cohorts to PostgreSQL"""
        
        print("\nWriting cohorts to PostgreSQL...")
        start_time = time.time()
        
        jdbc_url = DatabaseConfig.get_jdbc_url()
        properties = DatabaseConfig.get_connection_properties()
        
        # Prepare for database (select only needed columns)
        cohorts_db = cohorts.select(
            to_date('cohort_month').alias('cohort_month'),
            'region',
            'age_bucket',
            'gender',
            'subscription_tier',
            'total_users',
            'active_users',
            'avg_lifetime_value',
            'avg_purchases',
            'created_at'
        )
        
        # Write to PostgreSQL
        cohorts_db.write \
            .jdbc(
                url=jdbc_url,
                table='user_cohorts',
                mode='overwrite',  # Replace existing cohorts
                properties=properties
            )
        
        elapsed = time.time() - start_time
        record_count = cohorts_db.count()
        
        print(f"✓ Written {record_count:,} cohorts to PostgreSQL in {elapsed:.2f} seconds")
    
    def run(self):
        """Execute cohort analysis pipeline"""
        
        print("\n" + "=" * 80)
        print("SPARK JOB: User Cohort Analysis")
        print("=" * 80)
        
        total_start = time.time()
        
        try:
            # Read users
            users = self.read_users_from_postgres()
            
            # Calculate cohorts
            cohorts = self.calculate_cohorts(users)
            
            # Show insights
            self.show_cohort_insights(cohorts)
            
            # Write to database
            self.write_cohorts_to_postgres(cohorts)
            
            total_elapsed = time.time() - total_start
            
            print("\n" + "=" * 80)
            print(f"✓ Cohort analysis completed in {total_elapsed:.2f} seconds")
            print("=" * 80)
            
        except Exception as e:
            print(f"\n✗ Job failed: {e}")
            raise
        
        finally:
            self.spark.stop()


if __name__ == "__main__":
    analyzer = CohortAnalyzer()
    analyzer.run()

