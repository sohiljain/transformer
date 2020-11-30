# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

import logging
import os
import sys
import gnupg
import boto3
import errno
from botocore.exceptions import ClientError

from utils.secret import Secret

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


# Delete the temporary files created during processing on S3
def s3_delete_file(s3_path, bucket):
    try:
        s3_resource.Bucket(bucket).objects.filter(Prefix=s3_path).delete()
        logger.info(f"Deleted s3://{bucket}/{s3_path}")
    except Exception as e:
        logger.error(f'{e} Cannot delete {s3_path}')


def download_s3_fileobj(bucket, object_name, file_name=None):
    """Downlaod a binary file from an S3 bucket

    :param file_name: File to download
    :param bucket: Bucket to download to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    logger.info(f"Copying file from s3://{bucket}/{object_name} to {file_name}")

    # If S3 file_name was not specified, use object_name
    if file_name is None:
        file_name = object_name

    # Download the file
    try:
        with open(file_name, 'wb') as f:
            response = s3_client.download_fileobj(bucket, object_name, f)
        logger.info(f"Copied file from s3://{bucket}/{object_name} to {file_name}")
    except ClientError as e:
        logger.error(e)
        sys.exit(1)
        return False
    return True


def copy_staging_files(s3_path, folder, filename, bucket, s3_archive_path):
    # Copy object A as object B
    logger.info((f"Copy staging file to archive"))
    try:
        s3_resource.Object(bucket, f'{s3_archive_path}/{folder}/{filename}').copy_from(
            CopySource=f'{bucket}/{s3_path}')
    except Exception as e:
        logger.error(f'{e} Error in copy staging file')
    logger.info((f"Done copy staging file to {s3_archive_path}/{folder}/{filename}"))


# Check for local path
def assert_file_exists(path, filename):
    """
    Checks if directory tree in path exists. If not it created them.
    :param path: the path to check if it exists
    """
    try:
        os.makedirs(path)
        logger.info(f"{path} created")

        source = f'{path}/{filename}'
        if os.path.isfile(source):
            os.remove(source)
            logger.info(f"{source} file deleted")
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


# Configuring gpg decrypter for Aurus & 1010
def gpg_decrytion(decryption_key=None, root_dir=None, bucket=None, gnupghome=None):
    decryption_key_file = f'{root_dir}/gpghome/{decryption_key}'
    download_s3_fileobj(bucket, object_name=f'dollargeneral/transformed/pgp_key/{decryption_key}',
                        file_name=decryption_key_file)
    gpg_decrypt = gnupg.GPG(gnupghome=gnupghome)
    gpg_decrypt.encoding = 'utf-8'
    with open(decryption_key_file, 'rb') as f:
        key_data = f.read()
    import_result = gpg_decrypt.import_keys(key_data)
    return gpg_decrypt


# Records count check before storing at gold path
def count_check(spark, s3a_bucket, s3_staging_path_1010, s3_tmp_path, table):
    df_archive = spark.read.csv(f'{s3a_bucket}/{s3_staging_path_1010}/{table}/', sep='|',
                                header=True,
                                nullValue='\\N')
    table = table.replace('trans_disc_xref', 'discounts')
    df_transformed = spark.read.csv(f'{s3a_bucket}/{s3_tmp_path}/{table}/', sep='|', header=True,
                                    nullValue='\\N')
    logger.info(f'{table} test begins')

    count_transactiontimestamp = df_transformed.where(
        "transactiontimestamp='' or transactiontimestamp is null").count() if table == 'line_item' else 0

    count_check_id = df_transformed.where("check_id='' or check_id is null").count() if table in ['line_item',
                    'transactions', 'tenders', 'discounts'] else 0

    if table == 'tenders':
        count_customer_name = df_transformed.select("customer_name").distinct().count()
        if count_customer_name < 20000:
            pass
            # send_sns_alert(f"Transformer: {table} Customer Name match Failure",
            #                f"""Customer name count in tenders : {count_customer_name}""")

    # Send alert if conditions are not met and exit
    if df_transformed.count() == df_archive.count() and count_transactiontimestamp == 0 and count_check_id == 0:
        logger.info("All validations successful")
        logger.info(f'Gold - {df_transformed.count()}')
        logger.info(f'Archive - {df_archive.count()}')
    else:
        # send_sns_alert(f"Transformer: {table} Metrics Failure",
        #                f"""Transformed record count : {df_transformed.count()} Archive row count : {df_archive.count()}""")
        sys.exit(1)


def send_sns_alert(subject, message):
    """ method used to send SNS alert on topic provided."""
    try:
        # getting sns topic arn from parameter store
        ssm = boto3.client('ssm', region_name='us-west-2')

        # check for EMR or ALERT SNS topic
        if subject == 'batch_pgp_decrypt':
            sns_topic_arn = os.getenv("EMR_SNS_PARAM")
        else:
            sns_topic_arn = ssm.get_parameter(Name=os.getenv("ALERT_SNS_PARAM"))['Parameter']['Value']

        # sending sns message for alerting on slack and email
        sns_client = boto3.client('sns', region_name='us-west-2')
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            Message=str(message)
        )
    except Exception as e:
        logger.error(f"Failed to publish SNS message {e}", exc_info=True)
        raise Exception(f"Failed to publish SNS message {e}")
