from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, TimestampType, DoubleType, StringType
from pyspark.sql.utils import AnalysisException

# datalake paths
bronze_landing = "s3a://data/bronze/landing/"
bronze_conformed_olist = "s3a://data/bronze/conformed/olist/"
bronze_qa_olist = "s3a://data/bronze/qa/olist/"

def main():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # ---
    # read from bronze_landing
    customers = spark.read.format("delta").load(bronze_landing + "olist_customers")
    geolocation = spark.read.format("delta").load(bronze_landing + "olist_geolocation")
    order_items = spark.read.format("delta").load(bronze_landing + "olist_order_items")
    order_payments = spark.read.format("delta").load(bronze_landing + "olist_order_payments")
    order_reviews = spark.read.format("delta").load(bronze_landing + "olist_order_reviews")
    orders = spark.read.format("delta").load(bronze_landing + "olist_orders")
    products = spark.read.format("delta").load(bronze_landing + "olist_products")
    prod_cat_translation = spark.read.format("delta").load(bronze_landing + "olist_product_category_name_translation")
    sellers = spark.read.format("delta").load(bronze_landing + "olist_sellers")

    # ---
    # Adjust table schemas

    # customers - no need to change schema

    # geolocation - change schema
    geolocation = geolocation.withColumn(
        "geolocation_lat", F.col("geolocation_lat").cast(DoubleType())
    ).withColumn(
        "geolocation_lng", F.col("geolocation_lng").cast(DoubleType())
    )

    # order_items - change schema
    order_items = order_items.withColumn(
        "order_item_id", F.col("order_item_id").cast(IntegerType())
    ).withColumn(
        "shipping_limit_date", F.col("shipping_limit_date").cast(TimestampType())
    ).withColumn(
        "price", F.col("price").cast(FloatType())
    ).withColumn(
        "freight_value", F.col("freight_value").cast(FloatType())
    )

    # order_payments - change schema
    order_payments = order_payments.withColumn(
        "payment_sequential", F.col("payment_sequential").cast(IntegerType())
    ).withColumn(
        "payment_installments", F.col("payment_installments").cast(IntegerType())
    ).withColumn(
        "payment_value", F.col("payment_value").cast(FloatType())
    )

    # order_reviews - change schema
    order_reviews = order_reviews.withColumn(
        "review_score", F.col("review_score").cast(IntegerType())
    ).withColumn(
        "review_creation_date", F.col("review_creation_date").cast(TimestampType())
    ).withColumn(
        "review_answer_timestamp", F.col("review_answer_timestamp").cast(TimestampType())
    )

    # orders - change schema
    orders = orders.withColumn(
        "order_purchase_timestamp", F.col("order_purchase_timestamp").cast(TimestampType())
    ).withColumn(
        "order_approved_at", F.col("order_approved_at").cast(TimestampType())
    ).withColumn(
        "order_delivered_carrier_date", F.col("order_delivered_carrier_date").cast(TimestampType())
    ).withColumn(
        "order_delivered_customer_date", F.col("order_delivered_customer_date").cast(TimestampType())
    ).withColumn(
        "order_estimated_delivery_date", F.col("order_estimated_delivery_date").cast(TimestampType())
    )

    # product category translation - no need to change schema 

    # products - change schema
    products = products.withColumn(
        "product_name_lenght", F.col("product_name_lenght").cast(IntegerType())
    ).withColumn(
        "product_description_lenght", F.col("product_description_lenght").cast(IntegerType())
    ).withColumn(
        "product_photos_qty", F.col("product_photos_qty").cast(IntegerType())
    ).withColumn(
        "product_weight_g", F.col("product_weight_g").cast(FloatType())
    ).withColumn(
        "product_length_cm", F.col("product_length_cm").cast(FloatType())
    ).withColumn(
        "product_height_cm", F.col("product_height_cm").cast(FloatType())
    ).withColumn(
        "product_width_cm", F.col("product_width_cm").cast(FloatType())
    )

    # sellers - no need to change schema

    # ---
    # data quality
    # rows with null keys are sent to verification

    # customers
    customers_qa = customers.filter(F.col("customer_id").isNull())
    customers_qa = customers_qa.withColumn('reason', F.lit('customer id null')) if customers_qa.count() > 0 \
        else customers_qa.withColumn('reason', F.lit(None).cast(StringType()))
    
    try:
        customers_qa.write.format("delta").mode("append").save(bronze_qa_olist + "customers_qa")
    
    except AnalysisException:
        customers_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "customers_qa")

    # geolocation
    geolocation_qa = geolocation.filter(F.col("geolocation_zip_code_prefix").isNull())
    geolocation_qa = geolocation_qa.withColumn("reason", F.lit("zip code null")) if geolocation_qa.count() > 0 \
        else geolocation_qa.withColumn("reason", F.lit(None).cast(StringType()))

    try:
        geolocation_qa.write.format("delta").mode("append").save(bronze_qa_olist + "geolocation_qa")
    
    except AnalysisException:
        geolocation_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "geolocation_qa")

    # order_items
    order_items_qa = order_items.filter(
        (F.col("order_id").isNull()) | (F.col("product_id").isNull()) | (F.col("seller_id").isNull())
    )
    order_items_qa = order_items_qa.withColumn("reason", F.lit("order/cust/seller id null")) if order_items_qa.count() > 0 \
        else order_items_qa.withColumn("reason", F.lit(None).cast(StringType()))

    try:
        order_items_qa.write.format("delta").mode("append").save(bronze_qa_olist + "order_items_qa")
    
    except AnalysisException:
        order_items_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "order_items_qa")

    # order_payments
    order_payments_qa = order_payments.filter(F.col("order_id").isNull())
    order_payments_qa = order_payments_qa.withColumn("reason", F.lit("order id null")) if order_payments_qa.count() > 0 \
        else order_payments_qa.withColumn("reason", F.lit(None).cast(StringType()))
    
    try:
        order_payments_qa.write.format("delta").mode("append").save(bronze_qa_olist + "order_payments_qa")
    
    except AnalysisException:
        order_payments_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "order_payments_qa")

    # order_reviews
    order_reviews_qa = order_reviews.filter(
        (F.col("review_id").isNull()) | (F.col("order_id").isNull())
    )
    order_reviews_qa = order_reviews_qa.withColumn("reason", F.lit("review/order id null")) if order_reviews_qa.count() > 0 \
        else order_reviews_qa.withColumn("reason", F.lit(None).cast(StringType()))
    
    try:
        order_reviews_qa.write.format("delta").mode("append").save(bronze_qa_olist + "order_reviews_qa")
    
    except AnalysisException:
        order_reviews_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "order_reviews_qa")

    # orders
    orders_qa = orders.filter(
        (F.col("order_id").isNull()) | (F.col("customer_id").isNull())
    )
    orders_qa = orders_qa.withColumn("reason", F.lit("order or customer id null")) if orders_qa.count() > 0 \
        else orders_qa.withColumn("reason", F.lit(None).cast(StringType()))
    
    try:
        orders_qa.write.format("delta").mode("append").save(bronze_qa_olist + "orders_qa")
    
    except AnalysisException:
        orders_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "orders_qa")

    # prod_cat_translation
    prod_cat_translation_qa = prod_cat_translation.filter(F.col("product_category_name").isNull())
    prod_cat_translation_qa = prod_cat_translation_qa.withColumn("reason", F.lit("category null")) if prod_cat_translation_qa.count() > 0 \
        else prod_cat_translation_qa.withColumn("reason", F.lit(None).cast(StringType()))
    
    try:
        prod_cat_translation_qa.write.format("delta").mode("append").save(bronze_qa_olist + "prod_cat_translation_qa")
    
    except AnalysisException:
        prod_cat_translation_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "prod_cat_translation_qa")

    # products
    products_qa = products.filter(F.col("product_id").isNull())
    products_qa = products_qa.withColumn("reason", F.lit("product id null")) if products_qa.count() > 0 \
        else products_qa.withColumn("reason", F.lit(None).cast(StringType()))
    
    try:
        products_qa.write.format("delta").mode("append").save(bronze_qa_olist + "products_qa")
    
    except AnalysisException:
        products_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "products_qa")

    # sellers
    sellers_qa = sellers.filter(F.col("seller_id").isNull())
    sellers_qa = sellers_qa.withColumn("reason", F.lit("seller id null")) if sellers_qa.count() > 0 \
        else sellers_qa.withColumn("reason", F.lit(None).cast(StringType()))
    
    try:
        sellers_qa.write.format("delta").mode("append").save(bronze_qa_olist + "sellers_qa")
    
    except AnalysisException:
        sellers_qa.write.format("delta").mode("overwrite").save(bronze_qa_olist + "sellers_qa")

    # ---
    # conform tables - exclude records with null keys
    # customers
    customers = customers.filter(F.col("customer_id").isNotNull())

    # geolocation
    geolocation = geolocation.filter(F.col("geolocation_zip_code_prefix").isNotNull())

    # order_items
    order_items = order_items.filter(
        (F.col("order_id").isNotNull()) & (F.col("product_id").isNotNull()) & (F.col("seller_id").isNotNull())
    )

    # order_payments
    order_payments = order_payments.filter(F.col("order_id").isNotNull())

    # order_reviews
    order_reviews = order_reviews.filter(
        (F.col("review_id").isNotNull()) & (F.col("order_id").isNotNull())
    )

    # orders
    orders = orders.filter(
        (F.col("order_id").isNotNull()) & (F.col("customer_id").isNotNull())
    )

    # prod_cat_translation
    prod_cat_translation = prod_cat_translation.filter(F.col("product_category_name").isNotNull())

    # products
    products = products.filter(F.col("product_id").isNotNull())

    # sellers
    sellers = sellers.filter(F.col("seller_id").isNotNull())

    # save delta
    customers.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "customers")
    geolocation.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "geolocation")
    order_items.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "order_items")
    order_payments.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "order_payments")
    order_reviews.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "order_reviews")
    orders.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "orders")
    prod_cat_translation.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "product_category_name_translation")
    products.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "products")
    sellers.write.format("delta").mode("overwrite").save(bronze_conformed_olist + "sellers")

    spark.stop()

if __name__ == "__main__":
    main()