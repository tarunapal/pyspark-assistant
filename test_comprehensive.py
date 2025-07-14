#!/usr/bin/env python
"""
Comprehensive Test File for PySpark Assistant
This file contains code to test all features of the extension.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

print("Starting PySpark Assistant Comprehensive Test")

# SECTION 1: Initialize Spark with memory settings that will trigger analysis
spark = SparkSession.builder \
    .appName("PySpark Assistant Test") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.memory.fraction", 0.8) \
    .getOrCreate()

print("SparkSession created")

# SECTION 2: Create sample DataFrames of different sizes
# Small DataFrame
small_df = spark.range(100).toDF("id")
small_df = small_df.withColumn("value", F.rand() * 10)

# Medium DataFrame
medium_df = spark.range(10000).toDF("id")  # ~10MB
medium_df = medium_df.withColumn("value", F.rand() * 100)

# Large DataFrame
large_df = spark.range(1000000).toDF("id")  # ~100MB
large_df = large_df.withColumn("value", F.rand() * 1000)

# Another large DataFrame for joins
another_large_df = spark.range(2000000).toDF("id")  # ~200MB
another_large_df = another_large_df.withColumn("other_value", F.rand() * 200)

print("Sample DataFrames created")

# SECTION 3: Code Analysis Test Cases

# TEST 3.1: Multiple actions without caching
# This should trigger a warning about missing caching
result1 = medium_df.groupBy("id").count()  
result1.show()  # First action
result1.toPandas()  # Second action without caching in between

# TEST 3.2: Cartesian Join (critical warning)
# This should trigger a critical error about cartesian product
cross_joined = medium_df.crossJoin(small_df)  
cross_joined.show(5)

# TEST 3.3: Join without condition (implicit cartesian join)
# This should trigger an error about implicit cartesian product
bad_join = medium_df.join(small_df)  
bad_join.show(5)

# TEST 3.4: Collect on large DataFrame
# This should trigger a warning about collecting large data
all_data = large_df.collect()  # Will try to collect all rows to driver

# TEST 3.5: Converting large DataFrame to Pandas
# This should trigger a warning about memory usage
pandas_df = large_df.toPandas()  # Will try to convert all to Pandas DataFrame

# TEST 3.6: Multiple DataFrame scans without caching
# This should trigger a warning about repeated computation
expensive_df = medium_df.withColumn("expensive_calc", F.expr("value * 2 + id"))
count1 = expensive_df.count()  # First scan
avg_value = expensive_df.agg(F.avg("expensive_calc")).collect()[0][0]  # Second scan
max_value = expensive_df.agg(F.max("expensive_calc")).collect()[0][0]  # Third scan

# SECTION 4: Error Translation Test
# This will generate an actual error for testing the error translator
try:
    # Deliberately cause a Spark error (Division by zero)
    spark.createDataFrame([(0,)], ["value"]) \
        .selectExpr("1/value").collect()
except Exception as e:
    print("Error generated for testing:", str(e))
    # You can copy this error and use it in the Error Translator feature

# SECTION 5: Memory Management Test
# Operations that will trigger memory warnings due to large intermediate results
try:
    # Generate a skewed dataset
    skewed_df = spark.range(0, 1000000, 1).toDF("id") \
        .withColumn("key", F.when(F.col("id") < 100, 1).otherwise(F.col("id") % 100))
    
    # Perform a skewed join (will be inefficient)
    result = skewed_df.join(small_df, skewed_df.key == small_df.id)
    result.count()
except Exception as e:
    print("Memory management test error:", str(e))

# SECTION 6: Best Practices Test Cases
# These demonstrate how the code SHOULD be written

# Proper caching for multiple actions
print("\nBest practices demonstration:")
cached_df = medium_df.groupBy("id").count().cache()
cached_df.show()  # First action
cached_df_pandas = cached_df.limit(1000).toPandas()  # Second action, with limit
cached_df.unpersist()  # Clean up cache when done

# Proper join with condition
proper_join = medium_df.join(small_df, "id")
proper_join.show(5)

# Broadcast join for small DataFrame
broadcast_join = medium_df.join(F.broadcast(small_df), "id")
broadcast_join.show(5)

# Safe collect with limit
safe_data = large_df.limit(1000).collect()

# Safe conversion to pandas with sampling
safe_pandas = large_df.sample(False, 0.01).toPandas()

print("All tests completed!")
spark.stop() 