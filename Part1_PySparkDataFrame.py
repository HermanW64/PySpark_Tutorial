# The scripts cover dataframe manipulation

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySparkDataFrame").config("spark.ui.port", "4060").getOrCreate()

# read dataset
df_spark = spark.read.option("header","true").csv("pyspark_tutorial/datasets/test1.csv", inferSchema=True)
print("\nThe loaded test1.csv: \n")
df_spark.show()

# check schema
## By default, all columns are treated as string. 
## But you can use the parameter "inferSchema=True" when reading the dataset to automatically identify data types
print("\nCheck dataset schema:")
df_spark.printSchema()

# manipulations on columns
## show all columns -> list
print("\nShow all columns:\n", df_spark.columns)

## select a certain column by name: use a list in select()  
print("\nSelect the column Name:")
selected_column = df_spark.select(["Name", "Salary"])
selected_column.show()

## check data types for each column -> list
print("\nData types:\n", df_spark.dtypes)

## show description of a dataset -> dataframe
print("\nDescription of the dataset:\n")
df_spark.describe().show()

## create a column based on an existing column: withColumn(Name, operation)
df_spark_v1 = df_spark.withColumn("Experience after 2 years", df_spark['Experience']+2)
print("\nModified dataset:\n")
df_spark_v1.show()

## drop a column: drop(col_name) 
### You need to assign the result to a new variable, or replace the previous one
df_spark_v1 = df_spark_v1.drop("Experience after 2 years")
print("\nDrop the column Experience after 2 years: \n")
df_spark_v1.show()

## rename a column: withColumnRenamed(original_name, new_name)
df_spark_v1 = df_spark_v1.withColumnRenamed("Name", "New Name")
print("\n Rename the column Name to Rename: \n")
df_spark_v1.show()


