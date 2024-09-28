# Youtube video tutorial: https://youtu.be/_C8kWso4ne4?t=1947
# Start at 1:05:00

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("Groupby_Aggregate").getOrCreate()

# read dataset
training = spark.read.csv("pyspark_tutorial/datasets/test1.csv", header=True, inferSchema=True)
print("dataset test1.csv: \n")
training.show()
training.printSchema()

# assemble vector to prepare data
## You need to specify input columns (independent variables) and the output column (combined features)
## Then apply the vector assembler on a dataset
feature_assembler = VectorAssembler(inputCols=["age", "Experience"], outputCol="Independent Features")
output = feature_assembler.transform(training)
print("transformed dataset: \n")
output.show()
output.printSchema()

# before modelling, select the vector and the responsive variable
print("Only keep vectors and the responsive variable: \n")
finalized_data = output.select(["Independent Features", "Salary"])
finalized_data.show()

# split the dataset into training and test sets
training_set, test_set = finalized_data.randomSplit([0.75, 0.25])

# set independent and response features in regressor
regressor = LinearRegression(featuresCol="Independent Features", labelCol="Salary")
regression_result = regressor.fit(training_set)

# Print regression coefficients and intercept
print("The regression is:\n Salary = {0} + {1} * Age + {2} * Experience".
      format(round(regression_result.intercept,2), round(regression_result.coefficients[0],2), round(regression_result.coefficients[1],2)))