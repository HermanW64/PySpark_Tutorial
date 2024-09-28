# The scripts cover dataframe manipulation

# import libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import regexp_replace, col

spark = SparkSession.builder.appName("PySparkDataFrame_DataType").getOrCreate()

# use "format" to specify dataset format and "sep" for delimiter
df = spark.read.load("pyspark_tutorial\datasets\googleplaystore.csv", format="csv", sep=",", header=True, inferSchema=True)
df.show(5)

# Check schema and data cleaning
## drop some unuseful columns:
## drop() receives string only, not a list
df_dropped = df.drop("Size", "Content Rating", "Last Updated", "Android Ver")
print("Dataset schema:")
df_dropped.printSchema()

# perform data type conversion
## use withColumn() and cast(datatype method) to change data type
## you can also continue using withColumn() one by one
df_dropped_converted = df_dropped.withColumn("Reviews", col("Reviews").cast(IntegerType()))
df_dropped_converted = df_dropped_converted.withColumn("Rating", col("Rating").cast(DoubleType()))

## modify the column "Installs": remove "+"
df_dropped_converted = df_dropped_converted.withColumn("Installs", regexp_replace(col("Installs"), "[^0-9]", "")).withColumn("Installs", col("Installs").cast(IntegerType()))
df_dropped_converted = df_dropped_converted.withColumn("Price", regexp_replace(col("Price"), "[$]", "")).withColumn("Price", col("Price").cast(DoubleType()))

# check updated schema 
print("Updated dataset schema:")
df_dropped_converted.printSchema()
df_dropped_converted.show(5)




