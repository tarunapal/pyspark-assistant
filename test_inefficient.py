from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Inefficient PySpark Code") \
    .getOrCreate()

# Create some sample DataFrames
large_df = spark.range(1000000).toDF("id")
large_df = large_df.withColumn("value", F.rand() * 100)

another_large_df = spark.range(2000000).toDF("id")
another_large_df = another_large_df.withColumn("other_value", F.rand() * 200)

# ISSUE 1: Multiple actions without caching
result1 = large_df.groupBy("id").count()
result1.show()  # First action
result1.toPandas()  # Second action on same computation

# ISSUE 2: Cartesian Join (very inefficient)
cross_joined = large_df.crossJoin(another_large_df)
cross_joined.show(5)

# ISSUE 3: Join without condition (will cause cartesian product)
bad_join = large_df.join(another_large_df)
bad_join.show(5)

# ISSUE 4: Collect on large DataFrame without limiting
all_data = large_df.collect()  # This will try to bring all data to driver

# ISSUE 5: Converting large DataFrame to Pandas
pandas_df = large_df.toPandas()  # This will try to bring all data to driver memory

# ISSUE 6: GroupBy without analyzing key distribution
skewed_groups = large_df.groupBy("id").agg(
    F.sum("value").alias("total_value")
)
skewed_groups.show()

# ISSUE 7: Join without broadcast hint (inefficient for small-large df joins)
small_df = spark.range(100).toDF("id")
joined = large_df.join(small_df, "id")  # Should use broadcast for small_df
joined.show()

# ISSUE 8: Multiple DataFrame scans without caching
expensive_df = large_df.withColumn("expensive_calc", F.expr("value * 2 + id"))
count1 = expensive_df.count()  # First scan
avg_value = expensive_df.agg(F.avg("expensive_calc")).collect()[0][0]  # Second scan
max_value = expensive_df.agg(F.max("expensive_calc")).collect()[0][0]  # Third scan

# ISSUE 9: Inefficient use of collect() instead of aggregation
all_values = large_df.select("value").collect()  # Should use aggregation instead
total = sum(row.value for row in all_values)  # Doing aggregation in Python instead of Spark

spark.stop() 