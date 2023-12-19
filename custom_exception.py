"""
This module contains all the custom exception classes that will be raised in this lambda.
"""


class LambdaException(Exception):
    """
    This class serves as the base exception to all other exceptions in this module. All changes
    should be made in this class, as changes in this class will affect all the exceptions inheriting
    it.
    """

    def __init__(self, error_message, *args):
        """
        This init method simply wraps the init method of the base class. The only purpose of this
        method is to display the classname of the exception alongside the test message that was
        passed at the time of creation.
        """
        class_name = self.__class__.__name__
        super().__init__(f"{class_name}: {error_message}", *args)


class ImputationError(LambdaException):
    """
    This class is designed to be raised during any error while calling the imputation lambda.
    """


class S3WriteError(LambdaException):
    """
    This exception will be raised in case any error occurs while writing data to s3.
    """


class S3ReadError(LambdaException):
    """
    This exception will be raised in case any error occures while reading data from s3.
    """


class MissingReqParameter(LambdaException):
    """
    This exception will be raised in the case that a required parameter is missing from the event
    call.
    """


class InvalidParameter(LambdaException):
    """
    This exception will be raised in the case that a required parameter is missing from the event
    call.
    """
