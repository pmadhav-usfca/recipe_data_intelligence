import os 

# bucket_name = 'msds697_test'
bucket_name =os.environ.get("GS_BUCKET_NAME")

# service_account_key_file = '/Users/akhilgopi/airflow/dags/msds697.json'
service_account_key_file = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")

# mongo_username = 'admin'
mongo_username =os.environ.get("MONGO_USERNAME")
# mongo_password =  'MSDS697admin'
mongo_password =os.environ.get("MONGO_PASSWORD")
# mongo_ip_address = 'msds697-cluster.wqtcj.mongodb.net'
mongo_ip_address =os.environ.get("MONGO_IP")
# database_name = 'new_db'
database_name =os.environ.get("MONGO_DB_NAME")
# collection_name = 'reipes'
collection_name =os.environ.get("MONGO_COLLECTION_NAME")

all_recipes_api_url='https://www.allrecipes.com/servemodel/model.json?modelId=feedbacks&docId='
recipe_list_csv='recipe_list.csv'
recipe_index_pkl='recipe_index.pkl'
recipe_data_json='recipe_data.json'
failed_url_csv='failed_url.csv'
n_recipes=100

# mongodb+srv://admin:<password>@msds697-cluster.wqtcj.mongodb.net/