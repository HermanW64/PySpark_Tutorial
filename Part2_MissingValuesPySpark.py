# Youtube video tutorial: https://youtu.be/_C8kWso4ne4?t=1947
# Start at 32:00

from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer

# start a spark session
spark = SparkSession.builder.appName("MissingValuePractice").getOrCreate()

# load a dataset with missing values: test2.csv
df_spark = spark.read.csv("pyspark_tutorial/datasets/test2.csv", header=True, inferSchema=True)
print("\nDataset with missing values:\n", df_spark.show())

# A quick and simple way to remove all rows with missing values
df_spark_rm_all_na = df_spark.na.drop()
print("\nAll rows with missing values are dropped:\n")
df_spark_rm_all_na.show()

## more options: drop():
### how = "any": drop rows if it contains any nulls
### how = "all": drop rows only if all its values are nulls
### thresh = integer: drop rows if the number of non-nulls smaller than the integer
### subset = [col_1, col_2, ...]: drop rows only if there are nulls in certain columns

df_spark_rm_t3 = df_spark.na.drop(thresh=3)
print("\nRemove rows where there are less than 3 non-nulls:\n")
df_spark_rm_t3.show()

df_spark_rm_na_AgeExpSalary = df_spark.na.drop(subset=["age", "Experience", "Salary"])
print("\nRemove rows where any of age, experience, or salary is null:\n")
df_spark_rm_na_AgeExpSalary.show()

# fill missing value: na.fill(value, [col])
## fill nulls with another string (presquite: the column type is string)
df_spark_fillna_string = df_spark.na.fill("Missing Values")
print("\nReplace text nulls with strings:\n")
df_spark_fillna_string.show()

df_spark_fillna_zero = df_spark.na.fill(-1, ["age", "Experience", "Salary"])
print("\nReplace numeric nulls with zero:\n")
df_spark_fillna_zero.show()

## fill missing values with statistics
### define the imputer to fill nulls but keep original columns
imputer = Imputer(
    inputCols= ["age", "Experience", "Salary"],
    outputCols= ["{}_imputed".format(c) for c in ["age", "Experience", "Salary"]]
).setStrategy("mean")

### fit the imputer to the dataset
df_spark_imputer = imputer.fit(df_spark).transform(df_spark)
print("\nFill nulls with column means: \n")
df_spark_imputer.show()