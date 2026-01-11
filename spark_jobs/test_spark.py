"""
Test Spark Installation
Verifies PySpark is working correctly
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
import time


def test_spark_installation():
    """Test basic Spark functionality"""
    
    print("Testing Apache Spark Installation")
    print("=" * 60)
    
    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = SparkSession.builder \
        .appName("SparkInstallationTest") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    print(f"✓ Spark session created")
    print(f"  Spark version: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")
    
    # Test DataFrame operations
    print("\n2. Testing DataFrame operations...")
    
    # Create sample data
    data = [
        ("Alice", 34, "Engineer"),
        ("Bob", 45, "Manager"),
        ("Charlie", 28, "Analyst"),
        ("Diana", 52, "Director"),
        ("Eve", 31, "Engineer")
    ]
    
    df = spark.createDataFrame(data, ["name", "age", "role"])
    
    print(f"✓ Created DataFrame with {df.count()} rows")
    df.show()
    
    # Test aggregations
    print("\n3. Testing aggregations...")
    df.groupBy("role").agg(
        count("*").alias("count"),
        avg("age").alias("avg_age")
    ).show()
    
    # Test filters
    print("\n4. Testing filters...")
    df.filter(col("age") > 30).show()
    
    # Performance test
    print("\n5. Performance test (1 million rows)...")
    start_time = time.time()
    
    large_df = spark.range(1000000)
    result = large_df.filter(col("id") % 2 == 0).count()
    
    elapsed = time.time() - start_time
    
    print(f"✓ Processed 1M rows in {elapsed:.2f} seconds")
    print(f"  Result: {result:,} even numbers")
    
    # Stop Spark
    spark.stop()
    
    print("\n" + "=" * 60)
    print("✓ Spark installation test complete!")
    print("\nSpark is ready for use.")


if __name__ == "__main__":
    test_spark_installation()

