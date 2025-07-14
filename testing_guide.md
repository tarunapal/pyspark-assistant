# PySpark Assistant Testing Guide

This guide will help you test all features of the PySpark Assistant extension.

## Setup

1. Launch VS Code with the extension in debug mode:
   - Open your project in VS Code
   - Press `F5` to launch the Extension Development Host
   - You should see "PySpark Assistant is now active" in the Debug Console

## Test 1: Code Analysis

1. Open `test_comprehensive.py` in the Extension Development Host window
2. Look for red/yellow squiggly lines under problematic code
3. Hover over these squiggly lines to see detailed warnings with:
   - Data volume estimates
   - Performance impact estimates
   - Improvement suggestions

### What to look for:
- Hover over line 36: `result1.toPandas()` - Should warn about multiple actions without caching
- Hover over line 40: `cross_joined = medium_df.crossJoin(small_df)` - Should show critical warning about cartesian joins
- Hover over line 45: `bad_join = medium_df.join(small_df)` - Should warn about implicit cartesian join
- Hover over line 50: `all_data = large_df.collect()` - Should warn about collecting large data
- Hover over line 54: `pandas_df = large_df.toPandas()` - Should warn about memory usage

## Test 2: Error Translation

1. In the Extension Host window, press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac)
2. Type "PySpark: Translate Error" and press Enter
3. Paste this error message:
   ```
   org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (localhost executor driver): java.lang.ArithmeticException: / by zero
   ```
4. You should see a detailed error translation with:
   - Error summary
   - Explanation in plain English
   - Suggested solution

## Test 3: Dashboard View

1. Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac)
2. Type "PySpark: Open Dashboard" and press Enter
3. The dashboard should open with four sections:
   - Code Analysis
   - Error Translation
   - Spark UI Integration
   - Memory Management
4. Test the dashboard buttons:
   - Click "Analyze Current File" to analyze the open Python file
   - Click "Translate Error Message" to open the error translator

## Test 4: Problems Panel

1. Open the Problems panel (`Ctrl+Shift+M` or `Cmd+Shift+M`)
2. You should see all PySpark issues listed with:
   - Error messages
   - Warning messages
   - Information messages
   - Each should be from "PySpark Assistant" source

## Test 5: Memory Management

1. In `test_comprehensive.py`, look at lines 57-60:
   ```python
   expensive_df = medium_df.withColumn("expensive_calc", F.expr("value * 2 + id"))
   count1 = expensive_df.count()  # First scan
   avg_value = expensive_df.agg(F.avg("expensive_calc")).collect()[0][0]  # Second scan
   max_value = expensive_df.agg(F.max("expensive_calc")).collect()[0][0]  # Third scan
   ```
2. The extension should detect repeated computation without caching and suggest improvements

## Test 6: Best Practices Section

1. Look at the "Best Practices" section (lines 82-99)
2. These should NOT have warnings as they follow good PySpark practices:
   - Using `.cache()` for multiple actions
   - Using proper join conditions
   - Using broadcast joins for small DataFrames
   - Using `.limit()` before `.collect()`
   - Using `.sample()` before `.toPandas()`

## Troubleshooting

If you don't see analysis warnings:
1. Make sure the file has the `.py` extension
2. Try running the "PySpark: Analyze Current File" command manually
3. Check the Debug Console for any error messages
4. Try closing and reopening the Extension Development Host window

Remember: The extension analyzes code statically - it doesn't need to actually run the PySpark code to provide recommendations. 