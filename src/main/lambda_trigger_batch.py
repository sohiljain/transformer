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
_jobName = os.environ.get('BATCH_JOBNAME', 'cdp-dg-transformer')
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
            type = message.get('Type', None)
            schedule_name = message.get('Schedule', None)
            checks3flag = False
            logger.info(f'SNS Action - {action}')

            # Setting env variable as dev/prod
            if 'EnvDetail' in message:
                env_detail = message['EnvDetail']
            else:
                env_detail = 'dev' if os.environ['BRIDG_ENV_NAME'].split('-')[0] == 'dev' else 'prod'

            if action == "Start EMR Process Gold":
                try:
                    return_msg = trigger_emr_process_gold(env_detail, type, schedule_name)
                except Exception as e:
                    return_msg = "{type} Transformer EMR failed"
                    # send_sns_alert(return_msg, e)
                    logger.error(f"{return_msg} {e}", exc_info=True)

            elif action == "Manual":
                try:
                    date_value = message['DateValue']
                    return_msg = trigger_batch_pgp_decrypt(date_value, checks3flag, env_detail, type, schedule_name,
                                                           s3_key = None )

                except Exception as e:
                    return_msg = f"{type} Transformer Batch failed. Either batch failed or incorrect event passed"
                    # send_sns_alert(return_msg, e)
                    logger.error(f"{return_msg} {e}", exc_info=True)

            else:
                # This is the default daily prod run case. We want to check if all s3 files are present before starting
                checks3flag = True

                for internal_record in message['Records']:
                    s3_key = internal_record['s3']['object']['key']
                    logger.info(f's3_key: {s3_key}')

                    # Setting type to dg or popshelf
                    if 'popshelf' in s3_key:
                        type = 'popshelf'
                    else:
                        type = 'dg'

                    # Setting condition to exit if SNS triggered lambda for irrelevant s3 keys
                    if not (s3_key.startswith('dollargeneral/1010/Daily/') or s3_key.startswith(
                            'dollargeneral-popshelf/1010/Daily') or s3_key.startswith('dollargeneral/1010/Weekly/')
                            or s3_key.startswith('dollargeneral-popshelf/1010/Weekly/')):
                        raise Exception

                    # Setting schedule to weekly or daily by checking s3 key
                    if 'Weekly' in s3_key:
                        schedule = 'weekly'
                    elif 'Daily' in s3_key:
                        schedule = 'daily'
                    else:
                        raise Exception(f'schedule not found in s3_key {s3_key}')

                    file_name = s3_key.rsplit('_')[-1]
                    date_value = file_name.rsplit('_')[-1][:8]
                    logger.info(f'Processing {file_name} for {date_value}')

                    try:
                        return_msg = trigger_batch_pgp_decrypt(date_value, checks3flag, env_detail, type, schedule,
                                                               s3_key)
                    except Exception as e:
                        return_msg = f"{type} Transformer Batch failed"
                        # send_sns_alert(return_msg, e)
                        logger.error(f"{return_msg} {e}", exc_info=True)

    except Exception as e:
        return_msg = "Not appropriate event for Transformer"
        logger.error(f"{return_msg}. {e}", exc_info=True)

    return return_msg


def trigger_emr_process_gold(env_emr, type, schedule_name):
    """
    Trigger EMR cluster to process the staging files
    :return: Success/Failure message
    """
    connection = boto3.client('emr', region_name='us-west-2')
    logger.info(f"{env_emr} - Starting {type} Transformer pipeline")

    try:
        with open(f'/var/task/config/emr/{env_emr}/{env_emr}-{type}-{schedule_name}-emr.yml', 'r') as yml_file:
            emr_conf = yaml.safe_load(yml_file)
        jobName = emr_conf.get('Name', '')
        if is_another_emr_job_running(jobName):
            return "Terminating because another emr job is running"
        cluster_id = connection.run_job_flow(**emr_conf)
        response = f"Cluster created with the step..{cluster_id['JobFlowId']}"

    except Exception as e:
        response = "EMR Cluster cannot be started"
        logger.error(response)
        raise Exception(f"{response}. {e}")

    return response


def check_s3_files(date_value, type, s3_key, schedule):
    """
    Return True if S3 1010 files are present for the date_value else False
    :param date_value: date for which files to be processed
    :return: Return True if S3 1010 files are present for the date_value else False
    """
    s3_client = boto3.client('s3')
    bucket = os.getenv('S3_BUCKET_RAW_DATA_1')

    if schedule == 'daily' and (type == 'popshelf' or 'popshelf' in s3_key):
        remote_1010_path = 'dollargeneral-popshelf/1010/Daily'
    elif schedule == 'daily' and (type == 'dg' or 'dg' in s3_key):
        remote_1010_path = 'dollargeneral/1010/Daily'
    elif schedule == 'weekly' and (type == 'popshelf' or 'popshelf' in s3_key):
        remote_1010_path = 'dollargeneral-popshelf/1010/Weekly'
    elif schedule == 'weekly' and (type == 'dg' or 'dg' in s3_key):
        remote_1010_path = 'dollargeneral/1010/Weekly'
    else:
        logger.error(f'Invalid schedule: {schedule} or type {type} or s3_key {s3_key}')
        raise Exception(f'Invalid schedule: {schedule} or type {type} or s3_key {s3_key}')

    paginator = s3_client.get_paginator('list_objects')

    # All 1010 files that need to be checked
    list_files = ['transactions', 'tenders', 'transaction_item', 'trans_disc_xref', 'product', 'product_category',
                  'organization', 'discounts']
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


def trigger_batch_pgp_decrypt(date_value, checks3flag, env_batch, type, schedule, s3_key):
    """
    Code to start pgp_decrypt batch job
    :param env_batch: dev/prod
    :param date_value: date/date_range of file to be processed
    :return: Appropriate success/failure message
    """

    # check if S3 1010 files are present for the datevalue
    #TODO understand this
    if checks3flag == True:
        if not check_s3_files(date_value, type, s3_key, schedule):
            sys.exit(0)

    # starting pgp_decrypt batch job
    batch = boto3.client('batch', region_name='us-west-2')
    command = f'--module batch_pgp_decrypt --date {date_value} --env {env_batch} --type {type} --schedule {schedule}'
    logging.info(command)
    command = command.split()

    try:
        jobName = f'{_jobName}-{schedule}-popshelf' if 'popshelf' in type else f'{_jobName}-{schedule}'
        if 'backfill' in env_batch:
            jobName = f'backfill-{jobName}'

        if is_another_batch_job_running(jobName) or is_another_emr_job_running(f'{jobName}-emr-cluster'):
            return "Terminating because another batch or emr job is running"

        submit_job_response = batch.submit_job(
            jobName=jobName,
            jobQueue=jobQueue,
            jobDefinition=jobDefinition,
            containerOverrides={'command': command}
        )
        job_id = submit_job_response['jobId']
        batch_response_message = 'Submitted job {} {} to the job queue {}'.format(jobName, job_id, jobQueue)

    except Exception as e:
        logger.error(f'Failed to start Batch job')
        raise Exception(f"Failed to start Batch job {e}")

    return batch_response_message


def is_another_batch_job_running(jobName):
    batch_client = boto3.client('batch', region_name='us-west-2')
    for jobStatus in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
        response = batch_client.list_jobs(
            jobQueue=jobQueue,
            jobStatus=jobStatus
        )

        for jobSummary in response['jobSummaryList']:
            if jobSummary['jobName'] == jobName:
                logger.info(f"Found another batch job running for {jobName}")
                return True

    logger.info(f"No current batch job running for {jobName}")

    return False


def is_another_emr_job_running(jobName):
    emr_client = boto3.client('emr', region_name='us-west-2')

    response = emr_client.list_clusters(
        ClusterStates=[
            'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
        ]
    )
    for cluster in response['Clusters']:
        if cluster['Name'] == jobName:
            logger.info(f"Found another emr job running for {jobName}")
            return True

    logger.info(f"No current emr job running for {jobName}")

    return False
