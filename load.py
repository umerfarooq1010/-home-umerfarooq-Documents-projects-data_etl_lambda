"""
This module serves as a Load module in order to load data from code to an S3 location.
"""
import os
from io import StringIO
import boto3
import pandas as pd
from custom_exception import S3WriteError
from slack_api import send_slack_message
class Load:
    """This class serves as a helper class which provides methods to load data to an S3 location.
    """

    def __init__(self, bucket_name : str):
        """This function serves as an initializer to the Load class.

        Args:
            bucket_name (str): The bucket_name to load data to.
        """
        self.__bucket_name = bucket_name
        self.__s3_client = boto3.client('s3')
        self.__s3_resource = boto3.resource("s3")

    def __str__(self) -> str:
        """This function overwrites the __str__ method to print the credentials of the Extraction
           class

        Returns:
            str: The given string containing all the credential details.
        """
        return f"Bucket: {self.__bucket_name} | Client: {self.__s3_client} | \
Resource: {self.__s3_resource}"

    def write_to_s3(self, target_location : str, dataframe : pd.DataFrame) -> str:
        """This function writes a dataframe to a target location at S3. the written dataframe is
        in the format of a CSV.

        Args:
            target_location (str): The target location to store the dataframe to.
            dataframe (pd.DataFrame): The dataframe to store.

        Returns:
            str: Return the path to s3 on sucessful write.
        """
        try:
            print('write data module is running')
            print('Bucket name :', self.__bucket_name)
            print('Path of file :', target_location)
            filename_ext = os.path.splitext(target_location)
            uploading_s3_path = filename_ext[0] + "_deskew_transformation" + filename_ext[1]
            # Convert the DataFrame to CSV format (you can choose other formats as well)
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            self.__s3_resource.Object(self.__bucket_name, uploading_s3_path).put(
                Body=csv_buffer.getvalue())
            # send_slack_message("Data has been uploaded to S3")
            return uploading_s3_path
        except Exception as _e:
            raise S3WriteError('Error in reading file from S3, check extraction->\
            read_data_from_S3 module') from _e