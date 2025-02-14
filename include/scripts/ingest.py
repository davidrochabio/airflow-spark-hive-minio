import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# datalake paths
bronze_landing = "s3a://data/bronze/landing/"

def main():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    curr_date = datetime.today().strftime('%Y-%m-%d')

    olist_files = ["olist_customers", "olist_geolocation", "olist_order_items",
                   "olist_order_payments", "olist_order_reviews", "olist_orders",
                   "olist_products", "olist_sellers", "olist_product_category_name_translation"]
    
    files_in_path = glob.glob("/usr/local/include/data/*.csv")

    files_dict = {}
    # get file paths and clean file names for output
    for file_path in files_in_path:

        new_file_name = file_path.replace("/usr/local/include/data/", "") \
            .replace("_dataset.csv", "") if "olist" in file_path else file_path \
            .replace("/usr/local/include/data/", "olist_") \
            .replace("_dataset.csv", "") \
            .replace(".csv", "")

        files_dict[new_file_name] = [file_path, new_file_name] # index 0 is file_path and index 1 is output file name 

    # check if all olist files that are needed for the pipeline are present
    ct_olist_files = 0    
    for file in list(files_dict.keys()):
        if any(file in i for i in olist_files):
            ct_olist_files += 1
    
    assert ct_olist_files == 9, "Insert all olist files needed."
    
    # read csvs and write deltas to bronze landing
    for value in files_dict.values():
        df = spark.read.csv(value[0], header=True) # read from file_path in files_dict
        
        # insert loading datetime column
        df = df.withColumn("loading_date", F.lit(curr_date))

        df.write.format("delta").mode("overwrite").save(bronze_landing + value[1]) # write to output file name in files_dict
    
        print(bronze_landing + value[1])
    
    spark.stop()

if __name__ == "__main__":
    main()
