# Youtube video tutorial: https://youtu.be/_C8kWso4ne4?t=1947
# Start at 45:00

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dataFilter").getOrCreate()

# read the dataset
df_spark = spark.read.csv("pyspark_tutorial/datasets/test1.csv", header=True, inferSchema=True)
print("Dataset test1.csv loaded: \n")
df_spark.show()

# filter the records where Salary is smaller than 20000
print("Salary < 20k: \n")
df_spark.filter("Salary < 20000").show()

# You can only select part of all columns
print("Salary < 20k (Name and age only): \n")
df_spark.filter("Salary < 20000").select(["Name", "age"]).show()

# You can add more conditions for filtering, use () for each condition and connect using "|" or "&"
print("Salary > 20k but age < 30: \n")
df_spark.filter((df_spark["Salary"] >= 20000) & (df_spark["age"] < 30)).show()

# "~" means NOT operation in PySpark
print("Salary NOT greater than or equal to 20k: \n")
df_spark.filter(~(df_spark["Salary"] >= 20000)).show()

