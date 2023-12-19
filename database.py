from sqlalchemy import create_engine
import json
# -----------------------------------------------------------------------------


# Reading env variables
secret_name = ""
region_name = ""
session = boto3.session.Session()
client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # Retrieve AWS S3 credentials
try:
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
except ClientError as e:
    raise e

aws_secret_s3 = get_secret_value_response['SecretString']
secret_data_s3 = json.loads(aws_secret_s3)
db = secret_data_s3['db']
db_user = secret_data_s3['db_user']
password = secret_data_s3['password']
host = secret_data_s3['host']
dbname = secret_data_s3['dbname']
port = secret_data_s3['port']
schema = secret_data_s3['schema']



# Database connection
# database = db.Database(user, password, host, dbname, port)
DATABASE_URL = f"""postgresql://{db_user}:{password}@{host}:{port}/{dbname}"""
print(DATABASE_URL)

engine = create_engine(DATABASE_URL)
print(DATABASE_URL)
try:
    with engine.connect() as connection_str:
        print('Successfully connected to the PostgreSQL database')
except Exception as ex:
    print(f'Sorry failed to connect: {ex}')