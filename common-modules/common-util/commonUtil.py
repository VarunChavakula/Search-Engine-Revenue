"""
This file has all the common functions that are used.
"""
import logging
from botocore.exceptions import ClientError
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def send_email(email_from, email_to, subject, body):
    """
    This function sends email using ses service with body data as text.
    :param email_from:
    :param email_to:
    :param subject:
    :param body
    """
    LOGGER.info('Sending email')
    email_to_list = email_to.split(",")
    ses = boto3.client('ses')
    ses.send_email(
        Source=email_from,
        Destination={
            'ToAddresses': email_to_list
        },
        Message={
            'Subject': {
                'Data': subject
            },
            'Body': {
                'Text': {
                    'Data': body
                }
            }
        }
    )
    LOGGER.info("In commonUtil.send_email, Successfully sent the email to %s", email_to)


def send_email_with_html(email_from, email_to, subject, html_body):
    """
    This function sends email using ses service with body data as html.
    :param email_from:
    :param email_to:
    :param subject:
    :param body_text:
    :param html_body:
    :return:
    """
    LOGGER.info('Sending email')
    client = boto3.client('ses', region_name='us-east-1')
    body_text = " "
    client.send_email(
        Destination={
            'ToAddresses': [
                email_to
            ],
        },
        Message={
            'Body': {
                'Html': {
                    'Data': html_body,
                },
                'Text': {
                    'Data': body_text,
                },
            },
            'Subject': {
                'Data': subject,
            },
        },
        Source=email_from
    )
    LOGGER.info("In commonUtil.send_email_with_html, Successfully sent the email to %s", email_to)


def get_glue_table_definition(glue_client, aws_account_id, database_name, table_name):
    """
    Get table definition.
    """
    LOGGER.info('In commonUtil - get glue table definition.')
    table_definition = glue_client.get_table(
        CatalogId=aws_account_id,
        DatabaseName=database_name,
        Name=table_name
    )
    if table_definition['ResponseMetadata']['HTTPStatusCode'] == 200:
        return table_definition['Table']

    return None

def glue_create_csv_partition(s3_path, aws_account_id, database_name, glue_client, table_definition, partition_values):
    """
    This method creates partition.
    """
    try:
        LOGGER.info("In commonUtil - glue_create_csv_partition")
        LOGGER.info('S3 Path: %s', s3_path)
        response = glue_client.create_partition(
            CatalogId=aws_account_id,
            DatabaseName=database_name,
            TableName=table_definition['Name'],
            PartitionInput={
                'Values': partition_values,
                'StorageDescriptor': {
                    'Columns': table_definition['StorageDescriptor']['Columns'],
                    'Location': s3_path,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'Compressed': False,
                    'NumberOfBuckets': -1,
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        'Parameters': {
                            "separatorChar": table_definition['StorageDescriptor']['SerdeInfo']['Parameters']['separatorChar'],
                            "serialization.format": "1"
                        }
                    },
                    'BucketColumns': [],
                    'SortColumns': [],
                    'Parameters': {
                        'classification': 'csv',
                        'delimiter': table_definition['StorageDescriptor']['SerdeInfo']['Parameters']['separatorChar']
                    },
                    'SkewedInfo': {},
                    'StoredAsSubDirectories': False
                },
                'Parameters': {}
            }
        )
        LOGGER.info('Create partition response status code: %s',
                    response['ResponseMetadata']['HTTPStatusCode'])
        LOGGER.info('Partition created for glue table: %s', table_definition['Name'])
        LOGGER.info("Completed commonUtil - glue_create_csv_partition")
    except ClientError as cer:
        if cer.response['Error']['Code'] == 'AlreadyExistsException':
            LOGGER.info('Partition already exists for table: %s', table_definition['Name'])
            LOGGER.info('Client response status code: %s', cer.response['ResponseMetadata']['HTTPStatusCode'])
            LOGGER.info('Client code: %s', cer.response['Error']['Code'])
            LOGGER.info('Client mesaage: %s', cer.response['Error']['Message'])
        else:
            raise Exception(str(cer.response['Error']['Message']))
