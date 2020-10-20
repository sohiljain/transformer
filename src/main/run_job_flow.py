#!/usr/bin/env python
import logging
import time
import boto3
import sys
from botocore.exceptions import ClientError

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# EMR file upload to s3 bucket
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

bucket = 'bridg-client-ftp'
root_dir = '/code'
emr_local_path = f'{root_dir}/main/emr_process_gold_daily.py'
emr_upload_path = 'dollargeneral/emr_code/emr_process_gold_daily.py'
# emr_upload_path = 'bridg-binary-registry/bridg-dollargeneral-transformer/emr_process_gold_daily.py'

#today's date
from datetime import datetime
datetime_object = datetime.now()
dt = datetime_object.strftime("%Y-%m-%d-%H-%M-%S")


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        logger.info(f"Copied EMR file from {emr_local_path} to s3://{bucket}/{emr_upload_path}")
    except ClientError as e:
        logging.error(e)
        sys.exit(1)
        return False
    return True

upload_file(file_name=emr_local_path, bucket=bucket, object_name=emr_upload_path)

CODE_DIR = f"s3://bridg-client-ftp/{emr_upload_path}"

connection = boto3.client(
    'emr',
    region_name='us-west-2'
)

# Cluster initialization
conn = boto3.client('emr', region_name='us-west-2')

cluster_id = connection.run_job_flow(
    Name=f'sohil-cdp-emr-cluster-for-dg-transformer_{dt}',
    LogUri='s3://bridg-client-ftp/dollargeneral/transformed/logs',
    ReleaseLabel='emr-5.27.0',
    Applications=[
        {
            'Name': 'Spark'
        },
    ],
    Configurations=[
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
            ]
        }
    ],
    Instances={
        'HadoopVersion': 'Amazon 2.8.5',
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'VolumeType': 'gp2',
                                'SizeInGB': 20
                            },
                            'VolumesPerInstance': 1
                        },
                    ],
                    'EbsOptimized': True
                }
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.2xlarge',
                'InstanceCount': 3,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'VolumeType': 'gp2',
                                'SizeInGB': 20
                            },
                            'VolumesPerInstance': 1
                        },
                    ],
                    'EbsOptimized': True
                }
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'TASK',
                'InstanceType': 'm5.2xlarge',
                'InstanceCount': 2,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'VolumeType': 'gp2',
                                'SizeInGB': 150
                            },
                            'VolumesPerInstance': 1
                        },
                    ],
                    'EbsOptimized': True
                }
            }
        ],
        'Ec2KeyName': 'cdp-admin',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-080ca4bae7f28e8a9',
    },
    Steps=[
        {"Name": "transformer-" + time.strftime("%Y%m%d-%H:%M"),
         'ActionOnFailure': 'TERMINATE_CLUSTER',
         'HadoopJarStep': {
             'Properties': [
                 {
                     'Key': 'PYSPARK_PYTHON',
                     'Value': '/usr/bin/python3'
                 },
             ],
             'Jar': 'command-runner.jar',
             'Args': ['sudo', 'pip-3.6', 'install', 'boto3']
         }
         },
        {"Name": "transformer-" + time.strftime("%Y%m%d-%H:%M"),
         'ActionOnFailure': 'TERMINATE_CLUSTER',
         'HadoopJarStep': {
             'Properties': [
                 {
                     'Key': 'PYSPARK_PYTHON',
                     'Value': '/usr/bin/python3'
                 },
             ],
             'Jar': 'command-runner.jar',
             'Args': ["spark-submit", CODE_DIR]
         }
         }

    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
)

print('cluster created with the step...', cluster_id['JobFlowId'])




