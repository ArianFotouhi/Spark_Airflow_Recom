import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
import pandas as pd
import urllib.request
import zipfile
from sklearn.preprocessing import MinMaxScaler
import pickle

#Extract tasks
@task()
def download_data():
    url = "http://files.grouplens.org/datasets/movielens/ml-100k.zip"
    filename = "movielens.zip"
    urllib.request.urlretrieve(url, filename)
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall()
    


@task()
def get_data():
    df = pd.read_csv("ml-100k/u.data", sep="\t", header=None, names=["user_id", "item_id", "rating", "timestamp"])
    with open('/data/file.pickle', 'wb') as f:
        pickle.dump(df, f)

#Transformation tasks
@task()
def missing_val():
    with open('/path/to/shared/file.pickle', 'rb') as f:
        df = pickle.load(f)
    df.fillna(df.mean(), inplace=True)
    return df

@task()
def normalization(df):
    scaler = MinMaxScaler()
    df[["rating"]] = scaler.fit_transform(train[["rating"]])
    with open('/data/file.pickle', 'wb') as f:
        pickle.dump(df, f)
    

#Load
@task()
def data_load():
    with open('/path/to/shared/file.pickle', 'rb') as f:
        df = pickle.load(f)
    pass


#Model definer
@task()
def model_def():
    pass

#Train
@task()
def train():
    pass

#Test
@task()
def test():
    pass

@task()
def show_results():
    pass





with DAG(dag_id="product_etl_dag",schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5),catchup=False,  tags=["product_model"]) as dag:

    with TaskGroup("Extract", tooltip="Extract from data source") as extract:
        dl_data = download_data()
        received_data = get_data()
        
        dl_data >> received_data


    with TaskGroup("transform", tooltip="Transform data") as transform:
        
        miss_val = missing_val()
        norm_val = normalization(miss_val)
        
        [miss_val, norm_val]

    with TaskGroup("load", tooltip="Load to model") as load:
        loaded_data = data_load()
        
        loaded_data

    with TaskGroup("Model Definer", tooltip="Model Preparation") as model_definition:
        model = model_def()
        
        model

    with TaskGroup("Train", tooltip="Training phase") as training:
        trained_model = train()
        
        trained_model

    with TaskGroup("Test", tooltip="Testing phase") as testing:
        test_model = test()
        results = show_results()
        test_model >> results

    extract >> transform >> load >> model_definition >> training >> testing