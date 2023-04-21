# Spark_Airflow_Recom
A big data based recommender system developed on Spark and Airflow pipeline. The code structure of app.py file makes the app a proper choice for a scheduled data pipeline for ML models. 


If airflow is used for the first time, in terminal create a role:
<br>airflow users create \
    --username <USERNAME> \
    --password <PASSWORD> \
    --firstname <FIRSTNAME> \
    --lastname <LASTNAME> \
    --role Admin \
    --email <EMAIL>
Then users can be seen by
<br>airflow users list

To run the app, go through the directory of app.py then in terminal:
<br>airflow webserver -p 8080

Also, open another terminal and same directory, then in terminal:
<br>airflow scheduler
