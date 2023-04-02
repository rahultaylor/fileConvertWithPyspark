from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

csv_file = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("inputFiles//airtravel.csv")


csv_file.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("outputFiles//csvFile.parquet")

csv_file.show()
