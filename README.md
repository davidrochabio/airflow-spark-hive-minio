# PySpark Data Pipeline 
## Dev Environment with Docker Compose, Airflow, Spark, MinIO, Hive and Jupyter Lab-Notebooks. Delta Lake support.

- Airflow: https://airflow.apache.org/
- Spark: https://spark.apache.org/
- MinIO: https://min.io/
- Hive: https://hive.apache.org/
- Jupyter: https://jupyter.org/

### Steps

- Clone repo and enter repo folder.
- Download OList Dataset from Kaggle (all tables) and put inside the "data" folder: 
    - https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce or
    - https://www.kaggle.com/datasets/erak1006/brazilian-e-commerce-company-olist or
    - https://www.kaggle.com/datasets/npscul/olist-datasets or
- Inside repo folder run "docker compose up".
- Go to localhost:8080 (Aiflow UI) and enter credentials (user: airflow / password: airflow). Go to Admin tab, connections and add a new connection:
    - Connection Id: spark_conn
    - Connection Type: Spark
    - Host: spark://spark-master
    - Port: 7077

    -> Save

- In Airflow UI go to DAGs tab, click on the olist_pipeline Dag and run it (top right corner).

#### Pipeline (inside scripts folder)
- Ingest task: Ingest csv files and save them as delta tables in bronze layer.
- Conform task: - Enforce table schemas and clean data. Perform data quality tests and verification.
- Transform: - Create fact and dimension tables. Save files in delta format and create spark external tables in the metastore.

  
Other resources:
- localhost:4040 - Spark UI. Check your Spark Master, worker(s) and applications.
- localhost:8888 - Jupyter Lab. Get your lab token from "jupyter" container logs. With provided SparkSession config you're able to interact with your Spark cluster and Hive metastore (DBs, tables, views, etc). 
- Deltalake layers/tables and Hive metastore/warehouse are persisted inside ./include/data
- Airflow DB and Metastore DB are persisted in docker volumes. Check docker-compose file.

- You can also use other datasets and change dags/scripts to create your own pipeline. Enjoy.


