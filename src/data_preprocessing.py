from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data_preprocessing():
    spark = SparkSession.builder.appName("RetailSalesPreprocessing").getOrCreate()

    df = spark.read.option("header", "true") \
                   .option("multiLine", "true") \
                   .option("quote", '"') \
                   .option("escape", '"') \
                   .option("delimiter", ",") \
                   .csv("./data/raw")

    #droping na
    df = df.dropna()

    #change text to lowercase
    df = df.withColumn("ITEM DESCRIPTION", col("ITEM DESCRIPTION").cast(StringType())) \
        .withColumn("SUPPLIER", trim(lower(col("SUPPLIER")))) \
        .withColumn("ITEM CODE", trim(lower(col("ITEM CODE")))) \
        .withColumn("ITEM DESCRIPTION", trim(lower(col("ITEM DESCRIPTION")))) \
        .withColumn("ITEM TYPE", trim(lower(col("ITEM TYPE"))))
        

    #correct data types
    df = df.withColumn("YEAR", col("YEAR").cast("int")) \
        .withColumn("MONTH", col("MONTH").cast("int")) \
        .withColumn("RETAIL SALES", col("RETAIL SALES").cast("double")) \
        .withColumn("RETAIL TRANSFERS", col("RETAIL TRANSFERS").cast("double")) \
        .withColumn("WAREHOUSE SALES", col("WAREHOUSE SALES").cast("double"))

    #renaming columns
    df = df.withColumnRenamed("YEAR", "year") \
        .withColumnRenamed("MONTH", "month") \
        .withColumnRenamed("SUPPLIER", "supplier") \
        .withColumnRenamed("ITEM CODE", "item_code") \
        .withColumnRenamed("ITEM DESCRIPTION", "item_description") \
        .withColumnRenamed("ITEM TYPE", "item_type") \
        .withColumnRenamed("RETAIL SALES", "retail_sales") \
        .withColumnRenamed("RETAIL TRANSFERS", "retail_transfers") \
        .withColumnRenamed("WAREHOUSE SALES", "warehouse_sales")

    #add date column
    df = df.withColumn("month", format_string("%02d", col("month")))
    df = df.withColumn("date", to_date(concat_ws("-", col("year"), col("month"), lit("01")), "yyyy-MM-dd"))

    #writing to new csv file
    df.repartition(1).write.option("header", "true") \
        .option("quoteAll", "true") \
        .option("escape", '"') \
        .mode("overwrite") \
        .csv("./data/processed_retail_sales_data")

data_preprocessing()
