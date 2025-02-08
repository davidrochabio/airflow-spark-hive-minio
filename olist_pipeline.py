from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# spark submit config
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_endpoint = "http://minio:9000"

spark_conf = {
    "spark.jars.packages": "io.delta:delta-spark_2.12:3.3.0,io.delta:delta-storage:3.3.0,org.antlr:antlr4-runtime:4.9.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.780",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.access.key": minio_access_key,
    "spark.hadoop.fs.s3a.secret.key": minio_secret_key,
    "spark.hadoop.fs.s3a.endpoint": minio_endpoint,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.javax.jdo.option.ConnectionURL": "jdbc:postgresql://metastore-pg:5432/metastore_db",
    "spark.hadoop.javax.jdo.option.ConnectionDriverName": "org.postgresql.Driver",
    "spark.hadoop.javax.jdo.option.ConnectionUserName": "hive",
    "spark.hadoop.javax.jdo.option.ConnectionPassword": "password",
    "spark.hadoop.hive.metastore.uris": "thrift://metastore:9083"
}

@dag(
    schedule=None,
    catchup=False
)
def olist_pipeline():
    
    ingest = SparkSubmitOperator(
        task_id="ingest_data",
        conn_id="spark_conn",
        application="/usr/local/include/scripts/ingest.py",
        name="ingestData",
        conf=spark_conf,
        verbose=True
    )

    conform = SparkSubmitOperator(
        task_id="conform_data",
        conn_id="spark_conn",
        application="/usr/local/include/scripts/conform.py",
        name="conformData",
        conf=spark_conf,
        verbose=True
    )

    transform = SparkSubmitOperator(
        task_id="transform_data",
        conn_id="spark_conn",
        application="/usr/local/include/scripts/transform.py",
        name="transformData",
        conf=spark_conf,
        verbose=True
    )
    
    ingest >> conform >> transform

olist_pipeline()