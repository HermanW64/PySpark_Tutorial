## Python PySpark
## Library list
## -- pyspark
## -- numpy
## -- pandas
## --- Install Java JDK on OS, version <= 17

from pyspark.sql import SparkSession

# load test dataset using pandas
test_data = pd.read_csv("pyspark_tutorial/datasets/test_data.csv")
print("\nPandas dataset: \n", test_data)

# create a Spark session
spark = SparkSession.builder.appName("Practice").config("spark.ui.port", "4060").getOrCreate()

# read the test dataset and show it
## you need to specify the first row as the header by using option("header", "true"), if needed
## (alternative) You can add a parameter in csv(): header=True
df_spark = spark.read.option("header", "true").csv("pyspark_tutorial/datasets/test_data.csv")

# df only shows column info, use show() to see the entire dataset
print("\nSpark dataset (All): \n")
df_spark.show()

## when output the first X rows, it returns a list
print("\nThe first 2 rows: \n", df_spark.head(2))

## provide detail info about each column
print("\nColumn info:")
df_spark.printSchema()