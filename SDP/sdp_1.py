from pyspark import pipelines as dp
from pyspark.sql.functions import *

# create streaming for order table
@dp.table(
    name="order_bronze",
    comment="order bronze table"
)
def order_bronze():
    order_df = spark.readStream.table("dev.bronze.orders_raw")
    return order_df

# create materialized view for customers
@dp.materialized_view(
    name="customer_bronze",
    comment="customer bronze table"
)
def customer_bronze():
    cust_df = spark.read.table("dev.bronze.customer_raw")
    return cust_df

# Create a view
@dp.temporary_view(
    name = "joined_vw",
    comment = "Joined view"
)
def joined_view():
    df_ord = spark.read.table("order_bronze")
    df_cust = spark.read.table("customer_bronze")

    df_join = df_ord.join(df_cust,on=df_ord.o_custkey==df_cust.c_custkey,how="left")
    return df_join

# Create MV to add new column
@dp.materialized_view(
    name="orders_silver",
    comment = "Joined silver table"
)    
def joined_silver():
    df = spark.read.table("joined_vw").withColumn("__insert_date",current_timestamp())
    return df

# Aggregate based on c_mktsegment and find the count of order (c_orderkey)
@dp.table(
    name="orders_agg_gold",
    comment = 'orders aggergated table'
)
def orders_agg_gold():
    df = spark.read.table("orders_silver")
    df_final = df.groupBy(col("c_mktsegment")).agg(count(col("o_orderkey")).alias("count_orders"),sum(col("o_totalprice")).alias("total_orders")).withColumn("__insert_date",current_timestamp())
    return df_final

