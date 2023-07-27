"""
timestream_user.py

This module provides a class for handling write operations to AWS Timestream.
"""

import sys
import traceback
import boto3

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


class TimestreamHandler:
    """
    A class representing a connection to an AWS Timestream and associated operations.

    Methods:
        __init__(self, aws_access, aws_secret, aws_region='us-east-1')
            Initializes the TimestreamHandler instance.
        run_query(self, query_string):
            Executes a query in AWS Timestream using the provided query string.
        __parse_query_result(self, query_result):
            Parses the query result obtained from AWS Timestream into a list of dictionaries.
    """

    def __init__(self, aws_access, aws_secret, aws_region='us-east-1'):
        """
        Initializes the Timestream client.

        Reference:
            https://docs.aws.amazon.com/timestream/latest/developerguide
                /code-samples.write-client.html
                /code-samples.query-client.html
                /code-samples.run-query.html
        """

        self.session = boto3.Session()
        self.query_client = self.session.client('timestream-query',
                                aws_access_key_id=aws_access,
                                aws_secret_access_key=aws_secret,
                                region_name=aws_region)
        self.query_client = self.session.client('timestream-query',
                                aws_access_key_id=aws_access,
                                aws_secret_access_key=aws_secret,
                                region_name=aws_region)
        self.paginator = self.query_client.get_paginator('query')

    def select_records(self, query_string):
        status = {'statusCode': None, 'message': None}
        records = None
        
        try:
            response = self.query_client.query(QueryString=query_string)
            print(f"response: {response}")

            status['statusCode'] = response['ResponseMetadata']['HTTPStatusCode']
            status['message'] = f"SelectRecords HTTPStatusCode: {status['statusCode']}"
            # status['message'] += f"\nSuccess selecting records from table - {table_name}"

            # Process the response and extract the records
            # records = response['Rows']
            records = response

        except (BotoCoreError, ClientError) as err:
            status['statusCode'] = 500
            status['message'] = f"Unknown error occurred: {err}"
            status['message'] += f"\nSelectRecords HTTPStatusCode: {status['statusCode']}"
            # status['message'] += f"\nError selecting records from table - {table_name}"

        return records
        
    # def run_query(self, query_string) -> tuple:
    #     """
    #     Executes a query in AWS Timestream using the provided query string.

    #     Args:
    #         query_string (str): The query string to be executed in AWS Timestream.

    #     Returns:
    #         tuple: A tuple containing the query records and the HTTP status code.
    #             The records are a list of parsed query results.
    #             The status code indicates the result of the operation.
    #     """

    #     status_code = 200
    #     records = []

    #     try:
    #         page_iterator = self.paginator.paginate(QueryString=query_string)
    #         for page in page_iterator:
    #             records += self.__parse_query_result(page)

    #     except Exception as err:
    #         status_code = 500
    #         print("Status:", status_code)
    #         print("Message: Exception while running the query")
    #         print("Details:", str(err))
    #         traceback.print_exc(file=sys.stderr)

    #     return records, status_code


    # def __parse_query_result(self, query_result) -> list:
    #     """
    #     Parses the query result obtained from AWS Timestream into a list of dictionaries.

    #     Args:
    #         query_result (dict): The query result obtained from AWS Timestream.

    #     Returns:
    #         list: A list of dictionaries representing the parsed query result.
    #             Each dictionary contains column names as keys and corresponding values.
    #     """

    #     column_info = query_result['ColumnInfo']
    #     rows = query_result['Rows']

    #     results = []
    #     for row in rows:
    #         data = row['Data']
    #         values = []
    #         for i, column in enumerate(data):
    #             column_name = column_info[i]['Name']
    #             column_value = list(column.values())[0]
    #             values.append({column_name: column_value})
    #         results.append(values)

    #     return results
