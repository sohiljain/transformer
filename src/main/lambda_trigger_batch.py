# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

import json
import os, boto3, logging, yaml
import datetime as dt
from utils.utils import send_sns_alert
# Create logger
import sys

logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Set env values
jobName = os.environ.get('BATCH_JOBNAME', 'cdp-dg-transformer')
jobQueue = os.environ.get('BATCH_JOBQUEUE', 'cdp-que')
jobDefinition = os.environ.get('BATCH_JOBDEFINITION', 'cdp-dg-transformer')
date_value = dt.datetime.now().strftime('%Y%m%d')  # Set default date as Today. This will be overridden.


# Lambda declaration
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
            if isinstance(message, str):
                message = json.loads(message)

            action = message.get('Action', None)
            checks3flag = False
            logger.info(f'SNS Action - {action}')

            # Setting env variable as dev/prod
            if 'EnvDetail' in message:
                env_detail = message['EnvDetail']
            else:
                env_detail = 'dev' if os.environ['BRIDG_ENV_NAME'].split('-')[0] == 'dev' else 'prod'

            if action == "Start EMR Process Gold":
                try:
                    return_msg = trigger_emr_process_gold(env_detail)
                except Exception as e:
                    return_msg = "DG Transformer EMR failed"
                    send_sns_alert(return_msg, e)
                    logger.error(f"{return_msg} {e}", exc_info=True)

            elif action == "Manual":
                try:
                    date_value = message['DateValue']
                    return_msg = trigger_batch_pgp_decrypt(date_value, checks3flag, env_detail)

                except Exception as e:
                    return_msg = f"DG Transformer Batch failed. Either batch failed or incorrect event passed"
                    send_sns_alert(return_msg, e)
                    logger.error(f"{return_msg} {e}", exc_info=True)

            else:
                # This is the default daily prod run case. We want to check if all s3 files are present before starting
                checks3flag = True

                for internal_record in message['Records']:
                    s3_key = internal_record['s3']['object']['key']
                    logger.info(f's3_key: {s3_key}')

                    if s3_key.startswith('dollargeneral/1010/Daily/'):
                        file_name = s3_key.split('/')[4]
                        date_value = file_name.rsplit('_')[-1][:8]
                        logger.info(f'Processing {file_name} for {date_value}')

                        try:
                            return_msg = trigger_batch_pgp_decrypt(date_value, checks3flag, env_detail)
                        except Exception as e:
                            return_msg = "DG Transformer Batch failed"
                            send_sns_alert(return_msg, e)
                            logger.error(f"{return_msg} {e}", exc_info=True)

    except Exception as e:
        return_msg = "Not appropriate event for DG Transformer"
        logger.error(f"{return_msg}. {e}", exc_info=True)

    return return_msg


def trigger_emr_process_gold(env_emr):
    """
    Trigger EMR cluster to process the staging files
    :return: Success/Failure message
    """
    connection = boto3.client('emr', region_name='us-west-2')
    logger.info(f"{env_emr} - Starting Dollargeneral Transformer pipeline")

    try:
        with open(f'/var/task/{env_emr}-emr.yml', 'r') as yml_file:
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


def trigger_batch_pgp_decrypt(date_value, checks3flag, env_batch):
    """
    Code to start pgp_decrypt batch job
    :param env_batch: dev/prod
    :param date_value: date/date_range of file to be processed
    :return: Appropriate success/failure message
    """

    # check if S3 1010 files are present for the datevalue
    if checks3flag == True:
        if not check_s3_files(date_value):
            sys.exit(0)

    # starting pgp_decrypt batch job
    batch = boto3.client('batch')
    command = f'--module batch_pgp_decrypt --date {date_value} --env {env_batch}'
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
