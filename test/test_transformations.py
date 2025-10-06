import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import sum as _sum

# Initialize Spark
spark = SparkSession.builder.master("local").appName("Test_ETL").getOrCreate()

def test_aggregation():
    # Sample data
    data = [
        Row(id=1, name="Joe", amount=100),
        Row(id=1, name="Joe", amount=200),
        Row(id=2, name="Sam", amount=300)
    ]
    df = spark.createDataFrame(data)

    # Transformation
    agg_df = df.groupBy("id", "name").agg(_sum("amount").alias("total_amount"))

    # Collect results
    results = {row["id"]: row["total_amount"] for row in agg_df.collect()}

    # Assertions
    assert results[1] == 300
    assert results[2] == 300
