import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# ----------------- Logging Setup -----------------
logging.basicConfig(
    filename="../logs/etl.log",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("banking_etl")

# ----------------- Load Config -----------------
with open("../configs/db_config.json") as f:
    config = json.load(f)

# ----------------- Spark Session -----------------
spark = SparkSession.builder \
    .appName("Banking_ETL") \
    .enableHiveSupport() \
    .getOrCreate()

logger.info("Job Started: banking_etl")

try:
    # ----------------- Extract -----------------
    logger.info("Reading Customers from MySQL...")
    customers_df = spark.read.format("jdbc") \
        .option("url", config["mysql"]["url"]) \
        .option("dbtable", "customers") \
        .option("user", config["mysql"]["user"]) \
        .option("password", config["mysql"]["password"]) \
        .option("driver", config["mysql"]["driver"]) \
        .load()
    logger.info(f"Customers Loaded. Rows: {customers_df.count()}")

    logger.info("Reading Transactions from HDFS...")
    transactions_df = spark.read.csv("hdfs://namenode:8020/data/transactions.csv", header=True, inferSchema=True)
    logger.info(f"Transactions Loaded. Rows: {transactions_df.count()}")

    # ----------------- Transform -----------------
    logger.info("Joining Customers with Transactions...")
    joined_df = transactions_df.join(customers_df, transactions_df.customer_id == customers_df.id, "inner")
    logger.info(f"After Join. Rows: {joined_df.count()}")

    logger.info("Aggregating Transactions per Customer...")
    agg_df = joined_df.groupBy("id", "name").agg(_sum("amount").alias("total_amount"))
    logger.info(f"Aggregation Complete. Rows: {agg_df.count()}")

    # ----------------- Load -----------------
    hive_db = config["hive"]["database"]
    hive_table = config["hive"]["table"]
    logger.info(f"Writing to Hive Table {hive_db}.{hive_table}...")

    agg_df.write.mode("overwrite").saveAsTable(f"{hive_db}.{hive_table}")
    logger.info("Write Complete.")

except Exception as e:
    logger.error(f"Job Failed: {str(e)}", exc_info=True)
    raise

finally:
    logger.info("Job Completed.")
    spark.stop()
