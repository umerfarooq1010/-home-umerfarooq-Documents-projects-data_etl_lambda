import boto3
import pandas as pd
from database import engine
from load import Load

class Extraction:
    """This class serves as a helper class to extract data from S3 into your given code.
    It supports pandas dataframe as an object as well.
    """

    def __init__(self, bucket_name: str, access_key: str, secret_key: str, aws_region: str,s3_folder: str,target_folder:str):
        """This function serves as an initializer to the Lambda Extraction module.

        Args:
            bucket_name (str): The given bucket from where to extract data from.
        """

        self.__bucket_name = bucket_name
        self.__s3_client = boto3.client("s3")
        self.__s3_resource = boto3.resource("s3")
        self.__s3_folder = s3_folder
        self.__target_folder = target_folder
        self.__s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=aws_region
        )


    def __str__(self) -> str:
        """This function overwrites the __str__ method to print the credentials of the Extraction
           class

        Returns:
            str: The given string containing all the credential details.
        """
        return f"Bucket: {self.__bucket_name} | Client: {self.__s3_client} | \
        Resource: {self.__s3_resource}"

    def download_files_from_s3_folder(self):
        """This function downloads all files from a specific folder in S3.

        Args:
            folder_path (str): The path to the folder in S3.
            local_folder_path (str): The local folder path where files will be downloaded.
        """

        try:
            print("Download files module is running")
            print("Bucket name:", self.__bucket_name)
            print("Path of folder in S3:", folder_path)

            objects = self.__s3_client.list_objects_v2(Bucket=self.__bucket_name, Prefix=self.__s3_folder)

            for obj in objects.get("Contents", []):
                file_key = obj["Key"]

            pd_read = boto3.client('s3').get_object(Bucket=self.__bucket_name, Key=file_key)
            df = pd.read_csv(pd_read['Body'])
            print(df)

            for row in df.to_dict(orient='records'):
                if row.get('Query'):
                    query = row['Query']
                    data = engine.execute(query)
                    result = data.fetchall()
                    df = pd.DataFrame(result)
                    load_obj = Load(self.__bucket_name)
                    # Uploading the data to s3 bucket.
                    load_obj.write_to_s3(self.__target_folder, df)
        except Exception as e:
            print("Error in downloading files from S3:", e)


