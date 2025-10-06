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
with open("/Users/mantukumardeka/Desktop/DataEngineering/banking_etl/config/db_config.json") as f:
    config = json.load(f)

# ----------------- Spark Session -----------------
# The JAR file is inside the mysql-connector-j-9.4.0 directory
jar_path = "/Users/mantukumardeka/Desktop/DataEngineering/jars/mysql-connector-j-9.4.0/mysql-connector-j-9.4.0.jar"

spark = SparkSession.builder \
    .appName("Banking_ETL") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
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
    transactions_df = spark.read.csv(
        "/Users/mantukumardeka/Desktop/DataEngineering/banking_etl.transactions.csv",
        header=True,
        inferSchema=True
    )
    logger.info(f"Transactions Loaded. Rows: {transactions_df.count()}")

    # ----------------- Transform -----------------
    logger.info("Joining Customers with Transactions...")
    # Use column aliases to avoid ambiguity and select specific columns after join
    joined_df = transactions_df.alias("t").join(
        customers_df.alias("c"),
        col("t.customer_id") == col("c.customer_id"),
        "inner"
    ).select(
        col("c.customer_id"),
        col("c.name"),
        col("t.amount")
    )
    logger.info(f"After Join. Rows: {joined_df.count()}")

    logger.info("Aggregating Transactions per Customer...")
    agg_df = joined_df.groupBy("customer_id", "name").agg(
        _sum("amount").alias("total_amount")
    )
    logger.info(f"Aggregation Complete. Rows: {agg_df.count()}")

    # ----------------- Load -----------------
    hive_db = config["hive"]["database"]
    hive_table = config["hive"]["table"]

    # Create database if it doesn't exist
    logger.info(f"Creating Hive database {hive_db} if not exists...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    logger.info(f"Database {hive_db} is ready.")

    logger.info(f"Writing to Hive Table {hive_db}.{hive_table}...")
    agg_df.write.mode("overwrite").saveAsTable(f"{hive_db}.{hive_table}")
    logger.info("Write Complete.")

except Exception as e:
    logger.error(f"Job Failed: {str(e)}", exc_info=True)
    raise

finally:
    logger.info("Job Completed.")
    spark.stop()