import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from user_definition import *
from scrape_all_recipes import *
from google.cloud import storage

storage_client = storage.Client.from_service_account_json(service_account_key_file)
bucket = storage_client.bucket(bucket_name)

def _download_all_recipes_data():
    recipe_list_csv_file = bucket.blob(recipe_list_csv)
    with recipe_list_csv_file.open('r') as f:
        url_list=f.read().split('\n')
    pickle_data = bucket.blob(recipe_index_pkl)
    index=pickle.load(pickle_data.download_as_bytes())
    if len(url_list)>index:
        recipe_data,failed_urls,new_url_list=all_recipes_scrape_recipe_page(url_list,index,api_url,n_recipes)
        index+=len(recipe_data)+len(failed_urls)
        with pickle_data.open(mode='wb') as f:
            pickle.dump(index, f)
        with recipe_list_csv_file.open(mode='w') as f:
            f.write('\n'.join(new_url_list))
        recipe_data_json_file = bucket.blob(recipe_data_json)
        with recipe_data_json_file.open(mode='r') as f:
            recipe_data_curr=json.load(f)
        recipe_data_curr+=recipe_data
        with recipe_data_json_file.open(mode='w') as f:
            json.dump(recipe_data_curr,f)


with DAG(
    dag_id="msds697-task2",
    schedule='@daily',
    start_date=datetime(2023, 2, 23),
    catchup=False
) as dag:

    create_insert_aggregate = SparkSubmitOperator(
        task_id="aggregate_creation",
        packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
        conf={"spark.driver.userClassPathFirst":True,
             "spark.executor.userClassPathFirst":True,
            #  "spark.hadoop.fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            #  "spark.hadoop.fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            #  "spark.hadoop.fs.gs.auth.service.account.enable":True,
            #  "google.cloud.auth.service.account.json.keyfile":service_account_key_file,
             },
        verbose=True,
        application='aggregates_to_mongo.py'
    )
    download_all_recipes_data = PythonOperator(task_id = "download_all_recipes_data",
                                                  python_callable = _download_all_recipes_data,
                                                  dag=dag)
    download_all_recipes_data
    # download_sf_weather_data >> create_insert_aggregate


        
        
        