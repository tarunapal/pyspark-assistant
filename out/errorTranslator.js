"use strict";
/************ errorTranslator.ts ************/
Object.defineProperty(exports, "__esModule", { value: true });
exports.ErrorTranslator = void 0;
class ErrorTranslator {
    constructor() {
        this.errorPatterns = this.initializeErrorPatterns();
    }
    translateError(errorText) {
        for (const { pattern, translate } of this.errorPatterns) {
            const match = pattern.exec(errorText);
            if (match) {
                return translate(match);
            }
        }
        // Default generic translation if no pattern matches
        return {
            originalError: errorText,
            summary: "Unknown Spark error",
            explanation: "This error wasn't recognized by the PySpark Assistant. It might be related to your specific code or environment.",
            solution: "Review your code for logical errors, check data types, and ensure your Spark environment is properly configured.",
            links: [
                { title: "Spark Documentation", url: "https://spark.apache.org/docs/latest/" },
                { title: "Spark Troubleshooting Guide", url: "https://spark.apache.org/docs/latest/troubleshooting.html" }
            ]
        };
    }
    initializeErrorPatterns() {
        return [
            // Java Heap Space / Out of Memory errors
            {
                pattern: /java\.lang\.OutOfMemoryError:?\s*Java\s*heap\s*space/i,
                translate: (match) => ({
                    originalError: match[0],
                    summary: "Java Heap Space Out of Memory Error",
                    explanation: "The Java Virtual Machine (JVM) ran out of memory. This typically happens when trying to process too much data on a single executor or the driver. Common causes include: collecting large DataFrames to the driver, inefficient joins resulting in data explosion, or insufficient memory allocation.",
                    solution: "1. Increase executor/driver memory if possible\n2. Optimize your code to reduce memory usage\n3. Avoid collect() on large DataFrames, use take() or sampling\n4. Consider using broadcast joins for smaller DataFrames\n5. Increase the number of partitions to process less data per task",
                    codeExample: `# Instead of\nresult = large_df.collect()\n\n# Use limit or sampling\nresult = large_df.limit(1000).collect()\n# or\nresult = large_df.sample(False, 0.01).collect()\n\n# For joins with a small DataFrame, use broadcast hint\nfrom pyspark.sql import functions as F\nresult = large_df.join(F.broadcast(small_df), "join_key")`,
                    links: [
                        { title: "Spark Memory Management", url: "https://spark.apache.org/docs/latest/tuning.html#memory-management-overview" },
                        { title: "Best Practices for Memory Tuning", url: "https://spark.apache.org/docs/latest/tuning.html#memory-tuning" }
                    ]
                })
            },
            // Task not serializable
            {
                pattern: /org\.apache\.spark\.SparkException:.*Task\s*not\s*serializable/i,
                translate: (match) => ({
                    originalError: match[0],
                    summary: "Task Not Serializable Exception",
                    explanation: "Spark can't serialize (convert to bytes) some object that's being used in your code. This happens when you're trying to use a non-serializable object inside a Spark operation like map() or filter(). Common causes include using database connections, file handles, or other non-serializable objects in closures.",
                    solution: "1. Make sure all objects used in Spark operations are serializable\n2. Create necessary objects inside the closure rather than using external references\n3. For database connections, create them inside mapPartitions() rather than map()\n4. Use broadcast variables for read-only data that needs to be available to all workers",
                    codeExample: `# Instead of this (not serializable)
db_connection = create_connection()  # Non-serializable connection
def process_row(row):
    # Using external db_connection inside closure
    return query_data(db_connection, row.id)
    
result = df.rdd.map(process_row)

# Do this instead (serializable)
def process_row(row):
    # Create connection inside function
    db_connection = create_connection()
    result = query_data(db_connection, row.id)
    db_connection.close()
    return result
    
# Or better yet, use mapPartitions to reuse connections
def process_partition(partition):
    db_connection = create_connection()
    for row in partition:
        yield query_data(db_connection, row.id)
    db_connection.close()
    
result = df.rdd.mapPartitions(process_partition)`,
                    links: [
                        { title: "Spark Serialization Guide", url: "https://spark.apache.org/docs/latest/tuning.html#data-serialization" },
                        { title: "Understanding Closure Serialization", url: "https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures" }
                    ]
                })
            },
            // Job aborted due to stage failure
            {
                pattern: /org\.apache\.spark\.SparkException:.*Job\s*aborted\s*due\s*to\s*stage\s*failure/i,
                translate: (match) => ({
                    originalError: match[0],
                    summary: "Job Aborted Due to Stage Failure",
                    explanation: "This is a generic error indicating that a Spark job failed because one of its stages failed. The root cause is typically found in the stack trace, often related to memory issues, data serialization problems, or failures in user code.",
                    solution: "Check the full stack trace for more details about the specific error. Common solutions include:\n1. Look for 'caused by' sections in the error message for the root cause\n2. Inspect the Spark UI for the failed stage and task\n3. Check executor logs for detailed error messages\n4. Look for memory issues, null values, or data type mismatches",
                    codeExample: `# To debug these issues:
# 1. Add more detailed error handling in your code
df = df.na.drop()  # Handle potential null values

# 2. Inspect the data before problematic operations
df.printSchema()  # Check data types
df.show(5)  # Preview data

# 3. Use try/except blocks in UDFs to provide more context
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def my_function(x):
    try:
        # Your logic here
        return result
    except Exception as e:
        return f"Error: {str(e)}"
        
my_udf = udf(my_function, StringType())`,
                    links: [
                        { title: "Spark Job Execution", url: "https://spark.apache.org/docs/latest/job-scheduling.html" },
                        { title: "Debugging Spark Applications", url: "https://spark.apache.org/docs/latest/monitoring.html" }
                    ]
                })
            },
            // Shuffle fetch failed
            {
                pattern: /org\.apache\.spark\.shuffle\.FetchFailedException/i,
                translate: (match) => ({
                    originalError: match[0],
                    summary: "Shuffle Fetch Failed",
                    explanation: "This error occurs during the shuffle phase when a Spark task can't fetch shuffle data from another executor. This often happens when an executor is lost due to out-of-memory errors or other failures.",
                    solution: "1. Increase executor memory to prevent OOM errors\n2. Increase spark.shuffle.io.maxRetries (default is 3)\n3. Check if there are any network issues between nodes\n4. Look for OOM errors in executor logs that preceded this error\n5. Reduce the amount of data being shuffled by filtering early or using broadcast joins",
                    codeExample: `# Configuration to make shuffle more resilient
spark = SparkSession.builder \\
    .config("spark.shuffle.io.maxRetries", 5) \\
    .config("spark.shuffle.io.retryWait", "30s") \\
    .config("spark.memory.fraction", 0.8) \\
    .getOrCreate()

# Reduce shuffle data volume
# Instead of this
result = large_df1.join(large_df2, "key").groupBy("column").count()

# Do this - filter early
filtered_df1 = large_df1.filter(conditions)
filtered_df2 = large_df2.filter(conditions)
result = filtered_df1.join(filtered_df2, "key").groupBy("column").count()`,
                    links: [
                        { title: "Spark Shuffle Operations", url: "https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations" },
                        { title: "Tuning Spark Shuffle Settings", url: "https://spark.apache.org/docs/latest/tuning.html#data-locality" }
                    ]
                })
            },
            // Arithmetic overflow/underflow
            {
                pattern: /java\.lang\.ArithmeticException:.*(?:overflow|underflow)/i,
                translate: (match) => ({
                    originalError: match[0],
                    summary: "Arithmetic Overflow/Underflow Exception",
                    explanation: "The calculation resulted in a number too large (overflow) or too small (underflow) to be represented in the required data type. This often happens with integer operations or when converting between data types.",
                    solution: "1. Use larger data types (LongType instead of IntegerType)\n2. Cast to decimal or double for calculations that might overflow\n3. Check your data for extreme values\n4. Apply limits or filters to prevent extreme values",
                    codeExample: `from pyspark.sql.types import LongType, DecimalType
from pyspark.sql.functions import col, cast

# Instead of this (might overflow)
df = df.withColumn("product", col("large_number1") * col("large_number2"))

# Do this (avoids overflow)
df = df.withColumn("large_number1", col("large_number1").cast(LongType()))
df = df.withColumn("large_number2", col("large_number2").cast(LongType()))
df = df.withColumn("product", col("large_number1") * col("large_number2"))

# For even larger numbers, use decimal
df = df.withColumn("large_number1", col("large_number1").cast(DecimalType(38, 0)))
df = df.withColumn("product", col("large_number1") * col("large_number2"))`,
                    links: [
                        { title: "Spark SQL Data Types", url: "https://spark.apache.org/docs/latest/sql-ref-datatypes.html" },
                        { title: "Working with Different Types in Spark", url: "https://spark.apache.org/docs/latest/sql-programming-guide.html" }
                    ]
                })
            },
            // Null pointer exception 
            {
                pattern: /java\.lang\.NullPointerException/i,
                translate: (match) => ({
                    originalError: match[0],
                    summary: "Null Pointer Exception",
                    explanation: "The code encountered a null value where it expected a non-null object. This often happens when working with data that contains nulls but your code doesn't handle them properly.",
                    solution: "1. Add null checks in your UDFs and functions\n2. Use .na.fill() or .na.drop() to handle nulls in DataFrames\n3. Use the 'isNull' and 'isNotNull' functions in filters\n4. Check for null values in your data before processing",
                    codeExample: `from pyspark.sql.functions import col, when, lit, isnull

# Drop rows with nulls in important columns
df = df.na.drop(subset=["critical_column"])

# Or fill nulls with default values
df = df.na.fill({"string_col": "unknown", "int_col": 0})

# Safely handle nulls in computations
df = df.withColumn("safe_division", 
                  when(col("denominator").isNotNull() & (col("denominator") != 0),
                       col("numerator") / col("denominator"))
                  .otherwise(lit(0)))

# In UDFs
def safe_function(x):
    if x is None:
        return default_value
    return process(x)`,
                    links: [
                        { title: "Handling Missing Data in Spark", url: "https://spark.apache.org/docs/latest/sql-programming-guide.html#handling-null-values" },
                        { title: "Working with Null Values", url: "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#missing-data" }
                    ]
                })
            }
        ];
    }
}
exports.ErrorTranslator = ErrorTranslator;
//# sourceMappingURL=errorTranslator.js.map