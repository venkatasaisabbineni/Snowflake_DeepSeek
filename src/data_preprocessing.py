from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data_preprocessing():
    spark = SparkSession.builder.appName("BorderCrossingPreprocessing").getOrCreate()

    #reas csv
    df = spark.read.option("header", "true").csv("./data/border_crossing_data.csv")

    #convert date format
    df = df.withColumn("Date", to_date(col("Date"), "MMM yyyy"))

    #adding new columns
    df = df.withColumn("Month", month(col("Date"))) \
        .withColumn("Year", year(col("Date")))

    #drop na
    df = df.dropna()

    #change to lowercase and trim white spaces
    df = df.withColumn("Port Name", trim(lower(col("Port Name")))) \
        .withColumn("State", trim(lower(col("State")))) \
        .withColumn("Border", trim(lower(col("Border")))) \
        .withColumn("Measure", trim(lower(col("Measure"))))

    #remove Point,Date as i dont need it
    df = df.drop("Point")
    df = df.drop("Date")

    #check datatypes
    df = df.withColumn("Value", col("Value").cast("int")) \
        .withColumn("Latitude", col("Latitude").cast("double")) \
        .withColumn("Longitude", col("Longitude").cast("double")) \
        .withColumn("Port Code", col("Port Code").cast("int"))

    #renaming columns
    df = df.withColumnRenamed("Port Name", "port_name") \
        .withColumnRenamed("State","state") \
        .withColumnRenamed("Port Code", "port_code") \
        .withColumnRenamed("Border","border") \
        .withColumnRenamed("Month","month") \
        .withColumnRenamed("Year","year") \
        .withColumnRenamed("Measure","type_of_transport") \
        .withColumnRenamed("Value","value") \
        .withColumnRenamed("Latitude","latitude") \
        .withColumnRenamed("Longitude","longitude")

    #writing to csv
    df.repartition(1).write.option("header", "true") \
        .mode("overwrite") \
        .csv("./data/processed_border_crossing_data.csv")
    
data_preprocessing()