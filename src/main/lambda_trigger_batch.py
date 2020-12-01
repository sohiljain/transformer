# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

import json
import os, boto3, logging, yaml
import datetime as dt
from utils.utils import send_sns_alert
# create logger
import sys

logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

env_detail = os.environ['BRIDG_ENV_NAME'].split('-')[0]
jobName = os.environ.get('BATCH_JOBNAME', 'cdp-dg-transformer')
jobQueue = os.environ.get('BATCH_JOBQUEUE', 'cdp-que')
jobDefinition = os.environ.get('BATCH_JOBDEFINITION', 'cdp-dg-transformer')
date_value = dt.datetime.now().strftime('%Y%m%d')  # Set default date as Today. This will be overridden.


def lambda_handler(event, context):
    """
    # Entry point for DG transformer
    # Check for SNS event and trigger batch or EMR appropriate
    :param event: event json passed by sns
    :param context: lambda context
    :return: Success/Failure message
    """

    try:
        # Check the sns event and trigger batch vs emr appropriately
        for record in event['Records']:
            message = record['Sns']['Message']

            if message == "Start EMR Process Gold":
                try:
                    return_msg = trigger_emr_process_gold()
                except Exception as e:
                    return_msg = "DG Transformer EMR failed"
                    # send_sns_alert(return_msg, e)
                    logger.error(f"{return_msg} {e}", exc_info=True)

            elif message == "Manual":
                date_value = record['Sns']['DateValue']
                if ':' in date_value:
                    start_date = dt.datetime.strptime(date_value.split(':')[0], '%Y%m%d')
                    end_date = dt.datetime.strptime(date_value.split(':')[1], '%Y%m%d')
                    delta = end_date - start_date
                    try:
                        for i in range(delta.days + 1): # Adding 1 to include end_date
                            logger.info(f'Starting Batch for date - {start_date}')
                            date_arg = (str(start_date)).split(' ')[0].replace('-', '')
                            return_msg = trigger_batch_pgp_decrypt(date_arg)
                            start_date += dt.timedelta(days=1)

                    except Exception as e:
                        return_msg = "DG Transformer Batch failed"
                        # send_sns_alert(return_msg, e)
                        logger.error(f"{return_msg} {e}", exc_info=True)

            else:
                s3_key = record['Sns']['Message']['Records'][0]['s3']['object']['key']
                file_name = s3_key.split('/')[5]
                date_value = file_name.split('_')[2][:8]
                try:
                    return_msg = trigger_batch_pgp_decrypt(date_value)
                except Exception as e:
                    return_msg = "DG Transformer Batch failed"
                    # send_sns_alert(return_msg, e)
                    logger.error(f"{return_msg} {e}", exc_info=True)


    except Exception as e:
        return_msg = "Not appropriate event for DG Transformer"
        logger.error(f"{return_msg}. {e}", exc_info=True)

    return return_msg


def trigger_emr_process_gold():
    """
    Trigger EMR cluster to process the staging files
    :return: Success/Failure message
    """
    connection = boto3.client('emr', region_name='us-west-2')
    logger.info(f"{env_detail} - Starting Dollargeneral Transformer pipeline")

    try:
        with open(f'/var/task/{env_detail}-emr.yml', 'r') as yml_file:
            emr_conf = yaml.safe_load(yml_file)
        cluster_id = connection.run_job_flow(**emr_conf)
        response = f"Cluster created with the step..{cluster_id['JobFlowId']}"

    except Exception as e:
        response = "EMR Cluster cannot be started"
        logger.error(response)
        raise Exception(f"{response}. {e}")

    return response


def check_s3_files(date_value):
    """
    Return True if S3 1010 files are present for the date_value else False
    :param date_value: date for which files to be processed
    :return: Return True if S3 1010 files are present for the date_value else False
    """
    s3_client = boto3.client('s3')
    bucket = 'bridg-client-ftp'
    remote_1010_path = 'dollargeneral/1010/Daily'
    paginator = s3_client.get_paginator('list_objects')

    # All 1010 files that need to be checked
    list_files = ['transactions', 'product', 'product_category', 'organization', 'tenders', 'transaction_item',
                  'trans_disc_xref', 'discounts']
    try:
        for folder in list_files:
            for result in paginator.paginate(Bucket=bucket,
                                             Prefix=f'{remote_1010_path}/{folder.title()}/bridg_{folder}_{date_value}'):
                for content in result['Contents']:
                    filename = content['Key'].split('/')[-1]
                    logger.info(filename)

        return_flag = True

    except Exception as e:
        return_flag = False
        logger.error(f'{folder} not found')

    return return_flag


def trigger_batch_pgp_decrypt(date_value):
    """
    Code to start pgp_decrypt batch job
    :param date_value: date/date_range of file to be processed
    :return: Appropriate success/failure message
    """

    # check if S3 1010 files are present for the datevalue
    if not check_s3_files(date_value):
        sys.exit(0)

    # starting pgp_decrypt batch job
    batch = boto3.client('batch')
    command = f'--module batch_pgp_decrypt --date {date_value} --env {env_detail}'
    command = command.split()

    try:
        submit_job_response = batch.submit_job(
            jobName=jobName,
            jobQueue=jobQueue,
            jobDefinition=jobDefinition,
            containerOverrides={'command': command}
        )
        job_id = submit_job_response['jobId']
        batch_response_message = 'Submitted job {} {} to the job queue {}'.format(jobName, job_id, jobQueue)

    except Exception as e:
        logger.error(f'Failed to start EMR job')
        raise Exception(f"Failed to start EMR job {e}")

    return batch_response_message

# def send_sns_alert(subject, error_message):
#     """
#     method used to send SNS alert on topic provided.
#     :param subject: Subject for message to be displayed in response
#     :param error_message: Error message to be displayed in response
#     :return: Appropriate success/failure alert
#     """
#     try:
#         # getting sns topic arn from parameter store
#         ssm = boto3.client('ssm', region_name='us-west-2')
#         sns_topic_arn = ssm.get_parameter(Name=os.getenv("ALERT_SNS_PARAM"))['Parameter']['Value']
#
#         # sending sns message for alerting on slack and email
#         sns_client = boto3.client('sns', region_name='us-west-2')
#         sns_client.publish(
#             TopicArn=sns_topic_arn,
#             Subject=subject,
#             Message=str(error_message)
#         )
#     except Exception as e:
#         logger.error(f"Failed to publish SNS message {e}", exc_info=True)
#         raise Exception(f"Failed to publish SNS message {e}")
