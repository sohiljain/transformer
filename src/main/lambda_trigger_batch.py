import json
import os, boto3, logging, yaml
import datetime as dt

# create logger
logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

env_detail = os.environ['BRIDG_ENV_NAME'].split('-')[0]
jobName = os.environ.get('BATCH_JOBNAME', 'cdp-dg-transformer')
jobQueue = os.environ.get('BATCH_JOBQUEUE', 'cdp-que')
jobDefinition = os.environ.get('BATCH_JOBDEFINITION', 'cdp-dg-transformer')
date_value = dt.datetime.now().strftime('%Y%m%d') # Set default date as Today. This will be overridden.


# lambda handler
def lambda_handler(event, context):
    """
    Parse through all s3 dollargeneral 1010 files
    If all files for the date passed are present, then
    Trigger pgp_decrypt batch job
    :param event: event json passed by sns
    :param context: lambda context
    :return: Success/Failure message
    """

    try:
        for record in event['Records']:
            message = record['Sns']['Message']
            if message == "Start EMR Process Gold":
                return_msg = trigger_emr()
            else:
                for record in event['Records']:
                    s3_key = record['Sns']['Message']['Records'][0]['s3']['object']['key']
                    file_name = s3_key.split('/')[5]
                    date_value = file_name.split('_')[2][:8]
                    return_msg = check_s3_files(date_value)

    except Exception as e:
        return_msg = "Trigger not found"

    return return_msg


def trigger_emr():
    connection = boto3.client('emr', region_name='us-west-2')
    logger.info(f"{env_detail} - Starting Dollargeneral Transformer pipeline")

    try:
        with open(f'/var/task/{env_detail}-emr.yml','r') as yml_file:
            emr_conf = yaml.safe_load(yml_file)
        cluster_id = connection.run_job_flow(**emr_conf)
        response = f"Cluster created with the step..{cluster_id['JobFlowId']}"
    except:
        response = f"Cluster cannot be started"

    return response


def check_s3_files(date_value):
    s3_client = boto3.client('s3')
    bucket = 'bridg-client-ftp'
    remote_1010_path = 'dollargeneral/1010/Daily'
    paginator = s3_client.get_paginator('list_objects')

    list_files = ['transactions', 'product', 'product_category', 'organization', 'tenders', 'transaction_item',
                  'trans_disc_xref', 'discounts']
    try:
        for folder in list_files:
            for result in paginator.paginate(Bucket=bucket,
                                             Prefix=f'{remote_1010_path}/{folder.title()}/bridg_{folder}_{date_value}'):
                for content in result['Contents']:
                    filename = content['Key'].split('/')[-1]
                    logger.info(filename)

        return_msg = kickoff_transfer_batch(date_value)

    except Exception as e:
        return_msg = f'{folder} file not found'

    return return_msg


def kickoff_transfer_batch(date_value):
    """
    Code to start pgp_decrypt batch job
    :return: Appropriate success/failure message
    """
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
    except Exception as err:
        # send_sns_alert(subject='', message="Start EMR Process gold")
        batch_response_message = "error: " + str(err)

    return batch_response_message


def send_sns_alert(subject, error_message):
    """ method used to send SNS alert on topic provided."""
    try:
        # getting sns topic arn from parameter store
        ssm = boto3.client('ssm', region_name='us-west-2')
        sns_topic_arn = ssm.get_parameter(Name=os.getenv("ALERT_SNS_PARAM"))['Parameter']['Value']

        # sending sns message for alerting on slack and email
        sns_client = boto3.client('sns', region_name='us-west-2')
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            Message=str(error_message)
        )
    except Exception as e:
        logger.error(f"Failed to publish SNS message {e}", exc_info=True)
        raise Exception(f"Failed to publish SNS message {e}")
