import json
import os, boto3, logging, yaml
import datetime as dt
from utils.utils import send_sns_alert

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Today's date
date_value = dt.datetime.now().strftime('%Y%m%d')


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
    return event
    # if type(event)!='dict':
    #     pass
    #
    # if event.key() == 'Records'
    #     s3_key = event['Records']['s3']['bucket']['name']['s3']['bucket']['name']:
    # #     Then call the check_s3_files(s3_key)
    # elif:
    #     event.key() == 'emr':
    #     trigger_emr()
    # else:
    #     raise ValueError()

    # for record in event['Records']:
    #     bucket = record['s3']['bucket']['name']['s3']['bucket']['name']
    #     key = record['s3']['object']['key'])
    #     print(bucket)
    #     print(key)

def trigger_emr():

    connection = boto3.client('emr', region_name='us-west-2')
    logging.info("Starting Dollargeneral Transformer pipeline")

    with open(f'/Users/sjain/PycharmProjects/bridg-dollargeneral-transformer/src/config/emr.yml',
              'r') as yml_file:
        emr_conf = yaml.safe_load(yml_file)

    try:
        cluster_id = connection.run_job_flow(**emr_conf)
        response = f"Cluster created with the step..{cluster_id['JobFlowId']}"
    except:
        response = f"Cluster cannot be started"

    return response


def check_s3_files():
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

        return_msg = kickoff_transfer_batch()

    except Exception as e:
        return_msg = f'{folder} file not found'

    return return_msg


def kickoff_transfer_batch():
    """
    Code to start pgp_decrypt batch job
    :return: Appropriate success/failure message
    """
    batch = boto3.client('batch')
    jobName = os.environ.get('BATCH_JOBNAME', 'cdp-dg-transformer')
    jobQueue = os.environ.get('BATCH_JOBQUEUE', 'cdp-que')
    jobDefinition = os.environ.get('BATCH_JOBDEFINITION', 'cdp-dg-transformer')
    command = f'--date {date_value}'
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
        send_sns_alert(f"Batch job failure: {jobName} Metrics Failure",
                       f"""Job ID : {job_id} Job Queue : {jobQueue}""")
        batch_response_message = "error: " + str(err)

    return batch_response_message
