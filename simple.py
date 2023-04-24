import urllib.request

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import pandas as pd

url = "http://files.grouplens.org/datasets/movielens/ml-100k/u.data"
urllib.request.urlretrieve(url, "u.data")
    
spark = SparkSession.builder.appName("Recommendation_App").getOrCreate()

data = spark.read.format("csv").option("sep", "\t").option("header", False).load("u.data")
data = data.select(col("_c0").alias("userId"), col("_c1").alias("itemId"), col("_c2").alias("rating"), col("_c3").alias("timestamp"))
data = data.toPandas().to_dict(orient='records')


(trainingData, testData) = data.randomSplit([0.8, 0.2])
    
trainingData = trainingData.fillna(trainingData.mean())
testData = testData.fillna(testData.mean())

assembler = VectorAssembler(inputCols=["userId", "itemId", "timestamp"], outputCol="features")
trainingData = assembler.transform(trainingData)
testData = assembler.transform(testData)

scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
scalerModel = scaler.fit(trainingData)
trainingData = scalerModel.transform(trainingData)
testData = scalerModel.transform(testData)

untrained_model = RandomForestRegressor(labelCol="rating", featuresCol="scaledFeatures", numTrees=10)
model = untrained_model.fit(trainingData)

predictions = model.transform(testData)
print(predictions)
spark.stop()