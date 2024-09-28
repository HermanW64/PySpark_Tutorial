# Youtube video tutorial: https://youtu.be/_C8kWso4ne4?t=1947
# Start at 53:00

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Groupby_Aggregate").getOrCreate()

# load the dataset
df_spark = spark.read.csv("pyspark_tutorial/datasets/test3.csv", header=True, inferSchema=True)
print("dataset test3.csv loaded: \n")
df_spark.show()

# use goupby and sum() to find out total salary of each person
print("Group by Name, and calculate the total salary for each group: \n")
df_spark.groupby("Name").sum("Salary").show()

print("Group by Departments, and calculate the total salary for each group: \n")
df_spark.groupby("Departments").sum("Salary").show()

print("Group by Departments, and calculate the average salary for each group: \n")
df_spark.groupby("Departments").mean("Salary").show()

print("Group by Departments, and count the total number of persons for each group: \n")
df_spark.groupby("Departments").count().show()

print("You can also directly apply aggregate function: \n")
df_spark.agg({'Salary': 'sum'}).show()