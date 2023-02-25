import json

from google.cloud import storage
from mongodb import *

from pyspark.sql import Row,SparkSession
from user_definition import *

def make_docid_id(x):
    x['_id']=x.pop('doc_id')
    return x

def insert_aggregates_to_mongo():   
    spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc=spark.sparkContext
    rdd = sc.textFile(f"gs://{bucket_name}/{recipe_data_json}").map(lambda x:json.loads(x)).flatMap(lambda x:x)\
            .map(lambda x:make_docid_id(x))
    mongodb = MongoDBCollection(mongo_username,
                                mongo_password,
                                mongo_ip_address,
                                database_name,
                                collection_name)

    for doc in rdd.collect():
        # print(doc)
        if mongodb.return_num_docs({'_id':doc['_id']})==0:
            mongodb.insert_one(doc)
    

if __name__=="__main__":
    insert_aggregates_to_mongo()
