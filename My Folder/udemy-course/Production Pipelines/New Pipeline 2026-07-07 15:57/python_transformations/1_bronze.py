from pyspark import pipelines as dp
from pyspark.sql import functions as f

dataset_path = spark.conf.get("dataset_path")

@dp.table(
    name = "orders_raw",
    comment = "The raw books orders, ingested from orders-raw"
)

def process_order():
    """ By default, the table name will be the same as the function's name.
        If we prefer, we can add the name parameter to the decorator function to change it/ specify
        a different table name -- line: 7
    """
    orders_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{dataset_path}/orders-json-raw")
    )

    return orders_df

@dp.materialized_view
def customers():
    """
    With materialized views, we cannot perform a streaming read with spark.readStream.
    Instead, we use spark.read to perform a batch read
    """
    customes_df = spark.read.json(f"{dataset_path}/customers-json")
    return customes_df
