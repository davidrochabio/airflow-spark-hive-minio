import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# datalake paths
bronze_landing = "s3a://data/bronze/landing/"

def main():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    curr_date = datetime.today().strftime('%Y-%m-%d')

    # clean filenames
    for file_name in glob.glob("/usr/local/include/data/*.csv"):
        new_file_name = file_name.replace("/usr/local/include/data/", "") \
            .replace("_dataset.csv", "") if "olist" in file_name else file_name \
            .replace("/usr/local/include/data/", "olist_") \
            .replace(".csv", "")
    
        df = spark.read.csv(file_name, header=True)
        
        # insert loading datetime column
        df = df.withColumn("loading_date", F.lit(curr_date))

        df.write.format("delta").mode("overwrite").save(bronze_landing + new_file_name)
    
        print(bronze_landing + new_file_name)
    
    spark.stop()

if __name__ == "__main__":
    main()