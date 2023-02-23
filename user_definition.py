import os 

bucket_name = os.environ.get("GS_BUCKET_NAME")
service_account_key_file = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")

mongo_username = os.environ.get("MONGO_USERNAME")
mongo_password =  os.environ.get("MONGO_PASSWORD")
mongo_ip_address = os.environ.get("MONGO_IP")
database_name = os.environ.get("MONGO_DB_NAME")
collection_name = os.environ.get("MONGO_COLLECTION_NAME")

all_recipes_api_url='https://www.allrecipes.com/servemodel/model.json?modelId=feedbacks&docId='
recipe_list_csv=''
recipe_index_pkl=''
recipe_data_json=''
n_recipes=10000