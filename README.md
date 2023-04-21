# Spark_Airflow_Recom
A big data based recommender system developed on Spark and Airflow pipeline. The code structure of app.py file makes the app a proper choice for a scheduled data pipeline for ML models. 

To run the app, go through the directory of app.py then in terminal:
<br>airflow webserver -p 8080

Also, open another terminal and same directory, then in terminal:
<br>airflow scheduler
