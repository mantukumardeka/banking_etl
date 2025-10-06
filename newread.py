from pyspark.sql import SparkSession

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

# Optional: Create a temporary view to query with SQL
data.createOrReplaceTempView("transactions_summary")
print("\n--- SQL QUERY EXAMPLE ---")
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
print("Top 10 customers by total amount:")
result.show(truncate=False)

# Optional: Register as a permanent table for future use
# print("\n--- REGISTERING AS HIVE TABLE ---")
# spark.sql("CREATE DATABASE IF NOT EXISTS banking")
# data.write.mode("overwrite").saveAsTable("banking.transactions_agg")
# print("Table registered successfully!")
#
# # Now you can query it
# print("\n--- QUERYING FROM HIVE TABLE ---")
# hive_data = spark.sql("SELECT * FROM banking.transactions_agg")
# hive_data.show()

spark.stop()