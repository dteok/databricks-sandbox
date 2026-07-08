from pyspark import pipelines as dp
from pyspark.sql import functions as F


# We define our gold table object as a materlialized view
@dp.materialized_view
def cn_daily_customer_books():
    return (
        spark.read.table("orders_cleaned")
        .filter(F.col("country") == "China")
        .groupBy(
            "customer_id",
            "f_name",
            "l_name",
            "country",
            F.date_trunc("DD", F.col("order_timestamp")).alias("order_date")
        )
        .agg(F.sum("quantity").alias("books_count")
        )
    )

@dp.materialized_view
def fr_daily_customer_books():
    return (
        spark.read.table("orders_cleaned")
        .filter(F.col("country") == "France")
        .groupBy(
            "customer_id",
            "f_name",
            "l_name",
            "country",
            F.date_trunc("DD", F.col("order_timestamp")).alias("order_date")
        )
        .agg(F.sum("quantity").alias("books_count")
        )
    )
