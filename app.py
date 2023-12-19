from typing import Dict
from extraction import Extraction
from load import Load
import json

def lambda_handler(event: Dict, context) -> Dict:
    """This is the main lambda handler for the deskew transformation, it will apply specific transformation method.
       This function aims to perform the following actions:
        1. Fetch file from S3 bucket,
        2. Read the configuration from the file
        3. Query the data from the database on the base of those configurations

    Args:
        event (Dict): The event dictionary to receive parameters from.
        context (_type_): unknown, need to check
    Returns:
        Dict: The dictionary containing status code and the output file path.
    """

    # List of required parameters.
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
    access_key = secret_data_s3['access_key']
    secret_key = secret_data_s3['secret_key']
    bucket_name = secret_data_s3['bucket_name']
    aws_region = secret_data_s3['aws_region']
    source_folder = secret_data_s3['source_folder']
    target_folder = secret_data_s3['target_folder']



    # load_obj = Load(bucket_name)

    # Creating the ETL objects for the relevant bucket.
    ext_obj = Extraction(bucket_name, access_key, secret_key, aws_region,source_folder,target_folder)

    dataframe = ext_obj.download_files_from_s3_folder(source_folder, "input")

    # Uploading the data to s3 bucket.
    # load_obj.write_to_s3(file_path, dataframe)


payload = json.dumps({"body":"params"})
lambda_handler(payload, {})
