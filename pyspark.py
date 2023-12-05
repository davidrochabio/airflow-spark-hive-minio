#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

def load_csv(filename, schema):
    df = spark.read.csv(filename, header=True, schema=schema)                       
    return df

schemaOrders = StructType([
    StructField("order_id", StringType(), True), \
    StructField("customer_id", StringType(), True), \
    StructField("order_status", StringType(), True), \
    StructField("order_purchase_timestamp", DateType(), True), \
    StructField("order_approved_at", DateType(), True), \
    StructField("order_delivered_carrier_date", DateType(), True), \
    StructField("order_delivered_customer_date", DateType(), True), \
    StructField("order_estimated_delivery_date", DateType(), True)   
])

orders = load_csv('olist_orders_dataset.csv', schemaOrders)

schemaItems = StructType([
    StructField("order_id", StringType(), True), \
    StructField("order_item_id", IntegerType(), True), \
    StructField("product_id", StringType(), True), \
    StructField("seller_id", StringType(), True), \
    StructField("shipping_limit_date", DateType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("freight_value", DoubleType(), True)   
])

items = load_csv('PATH_TO_FILE/olist_order_items_dataset.csv', schemaItems)

items.show(truncate=False)

joined1 = orders.join(items, 'order_id', 'outer')

schemaSeller = StructType([
    StructField("seller_id", StringType(), True), \
    StructField("seller_zip_code_prefix", IntegerType(), True), \
    StructField("seller_city", StringType(), True), \
    StructField("seller__state", StringType(), True)
])

seller = load_csv('C:\\Users\\MASTER\\Documents\\Projects\\eCommerce\\olist_sellers_dataset.csv', schemaSeller)

joined2 = joined1.join(seller, 'seller_id', 'left')

joined2.show()

joined2.printSchema()

pricesum = joined2.groupBy('order_id').sum('price').withColumnRenamed('sum(price)', 'sum_price').orderBy('sum_price', ascending=False).show(1)

sellersum = joined2.groupBy('seller_id').sum('price').withColumnRenamed('sum(price)', 'sum_price').orderBy('sum_price', ascending=False).show(1)

count_status = joined2.groupBy('order_status').count()

total = count_status.groupBy().sum().collect()[0][0]

def perc(a):
    a = (a / total)*100
    return a

udf = F.udf(lambda x: perc(x), DoubleType())

count_status = count_status.withColumn('perc', perc(F.col('count')))

percent = count_status.select(F.round(count_status['perc'], 2).alias('percentage'))

percent.show()

joined2.select('order_purchase_timestamp').agg({'order_purchase_timestamp': 'max'}).show()

joined2.filter(joined2['order_purchase_timestamp'] >= '2018-04-17').groupBy('seller_id').agg({'price': 'sum'}).withColumnRenamed('sum(price)', 'total_sales').orderBy('total_sales', ascending=False).show()

joined2.registerTempTable('Table')
sqlContext = SQLContext(spark)

sqlContext.sql("SELECT * from Table").show()

