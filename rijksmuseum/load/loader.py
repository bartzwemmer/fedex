# https://kontext.tech/article/369/read-xml-files-in-pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_metadata = spark.read.format("com.databricks.spark.xml") \
    .option("rowTag","oai_dc").load("file:/xmls/*.xml")
df_metadata.show()

df_header = spark.read.format("com.databricks.spark.xml") \
    .option("rowTag","header").load("file:/xmls/*.xml")
df_header.show()