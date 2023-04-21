from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
import urllib.request
import pickle

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

#Extract tasks
@task()
def download_data():
    url = "http://files.grouplens.org/datasets/movielens/ml-100k/u.data"
    urllib.request.urlretrieve(url, "u.data")
    


@task()
def get_data():
    
    #Spark session creation
    spark = SparkSession.builder.appName("Recommendation_App").getOrCreate()

    data = spark.read.format("csv").option("sep", "\t").option("header", False).load("u.data")
    data = data.select(col("_c0").alias("userId"), col("_c1").alias("itemId"), col("_c2").alias("rating"), col("_c3").alias("timestamp"))
    spark.stop()
    return data

#Transformation tasks
@task()
def split_data(data):
    (trainingData, testData) = data.randomSplit([0.8, 0.2])
    return [trainingData, testData]

@task()
def missing_val(input_list):
    trainingData=input_list[0]
    testData= input_list[1]

    trainingData = trainingData.fillna(trainingData.mean())
    testData = testData.fillna(testData.mean())
    return [trainingData, testData]

@task()
def normalization(input_list):
    trainingData=input_list[0]
    testData= input_list[1]

    spark = SparkSession.builder.appName("Recommendation_App").getOrCreate()

    assembler = VectorAssembler(inputCols=["userId", "itemId", "timestamp"], outputCol="features")
    trainingData = assembler.transform(trainingData)
    testData = assembler.transform(testData)

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    scalerModel = scaler.fit(trainingData)
    trainingData = scalerModel.transform(trainingData)
    testData = scalerModel.transform(testData)
    
    spark.stop()
    return [trainingData,testData] 


#Load
@task()
def data_load(input_list):
    trainingData=input_list[0]
    testData= input_list[1]

    with open('/data/train_data.pickle', 'wb') as f:
        pickle.dump(trainingData, f)

    with open('/data/test_data.pickle', 'wb') as f:
        pickle.dump(testData, f)


#Model definer
@task()
def init_model():
    spark = SparkSession.builder.appName("Recommendation_App").getOrCreate()

    model = RandomForestRegressor(labelCol="rating", featuresCol="scaledFeatures", numTrees=10)
    with open('/data/untrained_model.pickle', 'wb') as f:
        pickle.dump(model, f)
    
    spark.stop()
    

#Train
@task()
def train():
    with open('/data/train_data.pickle', 'rb') as f:
       trainingData = pickle.load(f)
    with open('/data/untrained_model.pickle', 'rb') as f:
       untrained_model = pickle.load(f)
    model = untrained_model.fit(trainingData)
    
    
    with open('/data/model.pickle', 'wb') as f:
        pickle.dump(model, f)

    

#Test
@task()
def test():
    with open('/data/test_data.pickle', 'rb') as f:
       testData = pickle.load(f)
    with open('/data/model.pickle', 'rb') as f:
       model = pickle.load(f)
    
    predictions = model.transform(testData)
    
    return predictions

@task()
def show_results(predictions):
    predictions.show(10)
    







with DAG(dag_id="product_etl_dag",schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5),catchup=False,  tags=["product_model"]) as dag:

    with TaskGroup("ETL", tooltip="Extract Transform Load Data") as etl:
        dl_data = download_data()
        received_data = get_data()

        spt_data = split_data(received_data)
        miss_handle_data = missing_val(input_list = spt_data)
        norm_data = normalization(input_list = miss_handle_data)
        
        loaded_data = data_load(norm_data)

        [dl_data, received_data, miss_handle_data, norm_data ,loaded_data] 


    with TaskGroup("Model_Definer", tooltip="Model Preparation") as model_definition:
        model = init_model()
        
        model

    with TaskGroup("Train", tooltip="Training phase") as training:
        trained_model = train()
        
        trained_model

    with TaskGroup("Test", tooltip="Testing phase") as testing:
        test_pred = test()
        results = show_results(test_pred)
        test_pred >> results

    etl >> model_definition >> training >> testing