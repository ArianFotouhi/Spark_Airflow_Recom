import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
import pandas as pd

#Extract tasks
@task()
def download_data():
    pass


@task()
def get_data():
    pass

#Transformation tasks
@task()
def missing_val():
    pass

@task()
def normalization():
    pass

#Load
@task()
def data_load():
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
        
        transform_srcProductSubcategory = missing_val()
        transform_srcProductCategory = normalization()
        
        [transform_srcProductSubcategory, transform_srcProductCategory]

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