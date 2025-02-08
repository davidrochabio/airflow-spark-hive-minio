from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, TimestampType, DoubleType

# datalake paths
bronze_conformed_olist = "s3a://data/bronze/conformed/olist/"
silver = "s3a://data/silver/"

def main():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    #---
    # read tables from bronze_conformed
    customers = spark.read.format("delta").load(bronze_conformed_olist + "customers")
    geolocation = spark.read.format("delta").load(bronze_conformed_olist + "geolocation")
    order_items = spark.read.format("delta").load(bronze_conformed_olist + "order_items")
    order_payments = spark.read.format("delta").load(bronze_conformed_olist + "order_payments")
    order_reviews = spark.read.format("delta").load(bronze_conformed_olist + "order_reviews")
    orders = spark.read.format("delta").load(bronze_conformed_olist + "orders") 
    prod_cat_translation = spark.read.format("delta").load(bronze_conformed_olist + "product_category_name_translation")
    products = spark.read.format("delta").load(bronze_conformed_olist + "products")
    sellers = spark.read.format("delta").load(bronze_conformed_olist + "sellers")

    # create views to do transformations in SQL
    customers.createOrReplaceTempView("customers")
    geolocation.createOrReplaceTempView("geolocation")
    order_items.createOrReplaceTempView("order_items")
    order_payments.createOrReplaceTempView("order_payments")
    order_reviews.createOrReplaceTempView("order_reviews")
    orders.createOrReplaceTempView("orders")
    products.createOrReplaceTempView("products")
    prod_cat_translation.createOrReplaceTempView("prod_cat_translation")
    sellers.createOrReplaceTempView("sellers")
    
    # create fact tables
    # fact_order_items
    fact_order_items = spark.sql('''
    SELECT
        oi.order_id
        ,oi.order_item_id
        ,o.order_status
        ,o.customer_id
        ,oi.seller_id
        ,oi.product_id
        ,p.product_weight_g
        ,p.product_length_cm * p.product_height_cm * p.product_width_cm AS product_volume_cm3
        ,oi.price
        ,oi.freight_value
        ,o.order_purchase_timestamp
        ,o.order_approved_at
        ,oi.shipping_limit_date
        ,o.order_estimated_delivery_date
        ,o.order_delivered_carrier_date
        ,o.order_delivered_customer_date
    FROM order_items oi
    LEFT JOIN orders o
        ON oi.order_id = o.order_id
    LEFT JOIN products p
        ON oi.product_id = p.product_id
    ''')

    # fact_orders
    fact_orders = spark.sql('''
    WITH summed_order_items AS (
    SELECT
        order_id
        ,COUNT(product_id) AS qty_products
        ,COUNT(DISTINCT product_id) AS qty_distinct_products
        ,COUNT(DISTINCT seller_id) AS qty_distinct_sellers
        ,SUM(price) AS order_total
        ,SUM(freight_value) as order_freight_total
        ,MAX(shipping_limit_date) AS shipping_limit_date
    FROM order_items
    GROUP BY order_id
    ),
    avg_review AS (
        SELECT
            order_id
            ,COUNT(order_id) AS ct_reviews
            ,AVG(review_score) AS avg_review_score
        FROM order_reviews
        GROUP BY order_id
    ),
    payments AS (
    SELECT
        order_id
        ,COUNT(DISTINCT payment_type) AS ct_payment_types
        ,COLLECT_LIST(payment_type) AS payment_types
        ,SUM(payment_installments) AS payment_installments
        ,SUM(payment_value) AS total_payment
    FROM order_payments
    GROUP BY order_id
    )
    SELECT
        o.order_id
        ,o.customer_id
        ,o.order_status
        ,s.qty_products
        ,s.qty_distinct_products
        ,s.qty_distinct_sellers
        ,r.ct_reviews
        ,r.avg_review_score
        ,p.ct_payment_types
        ,p.payment_types
        ,p.payment_installments
        ,s.order_total
        ,s.order_freight_total
        ,p.total_payment
        ,o.order_purchase_timestamp
        ,o.order_approved_at
        ,s.shipping_limit_date
        ,o.order_estimated_delivery_date
        ,o.order_delivered_carrier_date
        ,o.order_delivered_customer_date
    FROM orders o
    LEFT JOIN summed_order_items s
        ON o.order_id = s.order_id
    LEFT JOIN avg_review r
        ON o.order_id = r.order_id
    LEFT JOIN payments p
        ON o.order_id = p.order_id
    ''')


    # create dimensions
    # dim_customer
    dim_customer = spark.sql('''
    SELECT
        c.customer_id
        ,c.customer_unique_id
        ,c.customer_zip_code_prefix
        ,c.customer_city
        ,c.customer_state
        ,g.geolocation_lat
        ,g.geolocation_lng
    FROM customers c
    LEFT JOIN geolocation g
        ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    ''')

    # dim_seller
    dim_seller = spark.sql('''
    SELECT
        s.seller_id
        ,s.seller_zip_code_prefix
        ,s.seller_city
        ,s.seller_state
        ,g.geolocation_lat
        ,g.geolocation_lng
    FROM sellers s
    LEFT JOIN geolocation g
        ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
    ''')

    # dim_product
    dim_product = spark.sql('''
    SELECT
        p.product_id
        ,p.product_category_name
        ,pt.product_category_name_english
        ,p.product_name_lenght
        ,p.product_description_lenght
        ,p.product_photos_qty
        ,p.product_weight_g
        ,p.product_length_cm
        ,p.product_height_cm
        ,p.product_width_cm
    FROM products p
    LEFT JOIN prod_cat_translation pt
        ON p.product_category_name = pt.product_category_name
    ''')

    # ---
    # write as external tables

    fact_order_items.write.format("delta") \
        .option("path", silver + "olist_fact_order_items") \
        .mode("overwrite") \
        .saveAsTable("olist_fact_order_items")
    
    fact_orders.write.format("delta") \
        .option("path", silver + "olist_fact_orders") \
        .mode("overwrite") \
        .saveAsTable("olist_fact_orders")
    
    dim_customer.write.format("delta") \
        .option("path", silver + "olist_dim_customer") \
        .mode("overwrite") \
        .saveAsTable("olist_dim_customer")
    
    dim_seller.write.format("delta") \
        .option("path", silver + "olist_dim_seller") \
        .mode("overwrite") \
        .saveAsTable("olist_dim_seller")

    dim_product.write.format("delta") \
        .option("path", silver + "olist_dim_product") \
        .mode("overwrite") \
        .saveAsTable("olist_dim_product")

    spark.stop()

if __name__ == "__main__":
    main()