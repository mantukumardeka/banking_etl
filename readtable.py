from pyspark.sql import SparkSession
import os
import shutil

# Create Spark session with Hive support and warehouse location
spark = SparkSession.builder \
    .appName("Reading from Hive in PyCharm") \
    .config("spark.sql.warehouse.dir",
            "/Users/mantukumardeka/Desktop/DataEngineering/banking_etl/scripts/spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

print("=" * 80)
print("Reading Parquet Data from Hive Warehouse")
print("=" * 80)

# Read the entire directory (handles all parquet files)
warehouse_path = "/Users/mantukumardeka/Desktop/DataEngineering/banking_etl/scripts/spark-warehouse/banking.db/transactions_agg/"

print(f"\nReading from: {warehouse_path}")
data = spark.read.format("parquet").load(warehouse_path)

print("\n--- DATA PREVIEW ---")
data.show(20, truncate=False)

print("\n--- SCHEMA ---")
data.printSchema()

print("\n--- SUMMARY STATISTICS ---")
data.describe().show()

print("\n--- RECORD COUNT ---")
print(f"Total records: {data.count()}")

# Create a temporary view to query with SQL (available only in this session)
data.createOrReplaceTempView("transactions_summary")

print("\n--- SQL QUERY EXAMPLE 1: Top 10 Customers by Total Amount ---")
result = spark.sql("""
    SELECT 
        customer_id,
        name,
        total_amount,
        ROUND(total_amount, 2) as rounded_amount
    FROM transactions_summary
    ORDER BY total_amount DESC
    LIMIT 10
""")
result.show(truncate=False)

print("\n--- SQL QUERY EXAMPLE 2: Average Transaction Amount ---")
avg_result = spark.sql("""
    SELECT 
        COUNT(*) as customer_count,
        ROUND(AVG(total_amount), 2) as avg_amount,
        ROUND(MIN(total_amount), 2) as min_amount,
        ROUND(MAX(total_amount), 2) as max_amount
    FROM transactions_summary
""")
avg_result.show(truncate=False)

print("\n--- SQL QUERY EXAMPLE 3: Customers with Amount > 5000 ---")
high_value = spark.sql("""
    SELECT 
        customer_id,
        name,
        ROUND(total_amount, 2) as total_amount
    FROM transactions_summary
    WHERE total_amount > 5000
    ORDER BY total_amount DESC
""")
print(f"Found {high_value.count()} high-value customers:")
high_value.show(truncate=False)

# Optional: Export to CSV
print("\n--- EXPORTING TO CSV ---")
output_path = "/Users/mantukumardeka/Desktop/DataEngineering/banking_etl/output"
data.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
print(f"Data exported to: {output_path}")

spark.stop()
print("\n" + "=" * 80)
print("Process completed successfully!")
print("=" * 80)