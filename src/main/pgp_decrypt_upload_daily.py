import boto3
import gnupg
import os, logging, sys
import datetime as dt
import yaml
import argparse
from utils.secret import Secret
from utils.config import EtlConfig
from utils.utils import download_s3_fileobj, copy_staging_files,assert_file_exists, gpg_decrytion

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# S3 object
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

# Download & Upload Aurus files from bridg-client-ftp to s3 transformed directory
def process_aurus(date_value=dt.datetime.now().strftime('%Y%m%d')):
    gpg_aurus = gpg_decrytion('aurus_decrypt_key.gpg', root_dir, bucket, gnupghome)
    paginator = s3_client.get_paginator('list_objects')

    # Iterate over directory
    for result in paginator.paginate(Bucket=bucket, Prefix=f'{remote_Aurus_path}/ADTFF_5_4_4_{date_value}'):
        for content in result.get('Contents', []):
            filename = content['Key'].split('/')[-1]
            folder = content['Key'].split('/')[-3]

            # Skip paths ending in /
            if not content['Key'].endswith('/'):
                local_file_absolute_path = f'{local_Path}/{filename}'

                s3_upload_file_path = f'''{s3_staging_path}/{folder.lower()}/{filename.replace('.pgp', '')}'''
                try:
                    # Make sure directories exist and file doesnot exist
                    assert_file_exists(local_Path, filename)

                    # Aurus - Download file to local path
                    s3_client.download_file(bucket, content['Key'], local_file_absolute_path)
                    logger.info(f'{local_file_absolute_path} downloaded')

                    # Decrypt aurus file and Upload to tranformed s3 directory
                    with open(local_file_absolute_path, 'rb') as f:
                        gpg_aurus.decrypt_file(f, passphrase=passphrase, output="tmp.csv")

                    logger.info(f'{local_file_absolute_path} decrypted to tmp.csv')

                    # Upload file to s3
                    s3_resource.meta.client.upload_file(Filename='tmp.csv', Bucket=bucket, Key=s3_upload_file_path)
                    logger.info(f'tmp.csv uploaded to {s3_upload_file_path}')
                except:
                    logger.error(f'Bad file {filename}')


# Download & Upload 1010 files from bridg-client-ftp to s3 transformed directory
def process_1010(date_value=dt.datetime.now().strftime('%Y%m%d')):
    """
    Downloads recursively the given S3 path to the target directory.
    :param client: S3 client to use.
    :param bucket: the name of the bucket to download from
    :param remote_1010_path: The S3 directory to download.
    :param local_Path: the local directory to download the files to.
    """
    gpg_1010 = gpg_decrytion('1010_decrypt_key.gpg', root_dir, bucket, gnupghome)
    paginator = s3_client.get_paginator('list_objects')

    list_files = ['transactions', 'product', 'product_category', 'organization', 'tenders', 'transaction_item', 'trans_disc_xref', 'discounts']

    for folder in list_files:
        for result in paginator.paginate(Bucket=bucket,
                                         Prefix=f'{remote_1010_path}/{folder.title()}/bridg_{folder}_{date_value}'):
            # Download each file individually
            for content in result['Contents']:

                filename = content['Key'].split('/')[-1]

                # Skip paths ending in /
                if not content['Key'].endswith('/'):
                    local_file_absolute_path = f'{local_Path}/{filename}'

                    try:
                        # Make sure file does not exist
                        assert_file_exists(local_Path, filename)

                        # Download file to local path
                        s3_client.download_file(bucket, content['Key'], local_file_absolute_path)
                        logger.info(f'{local_file_absolute_path} downloaded')

                        # Decrypt file
                        with open(local_file_absolute_path, 'rb') as f:
                            gpg_1010.decrypt_file(f, passphrase=passphrase, output="tmp.csv.gz")
                        logger.info(f'{local_file_absolute_path} decrypted to tmp.csv.gz')
                        os.remove(local_file_absolute_path)

                        # Upload file to staging by date partition
                        partition_col = filename.split('_')[-1][0:8]
                        s3_upload_file_path = f'''{s3_staging_path_1010}/{folder.lower()}/dt={partition_col}/{filename.replace('psv.gz.pgp', 'psv.gz')}'''
                        s3_resource.meta.client.upload_file(Filename='tmp.csv.gz', Bucket=bucket,
                                                            Key=s3_upload_file_path)
                        logger.info(f'tmp.csv.gz uploaded to {s3_upload_file_path}')

                        # Copying file from staging to archive directory to have a backup
                        copy_staging_files(s3_upload_file_path, folder, filename.replace('psv.gz.pgp', 'psv.gz'),
                                           bucket, s3_archive_path)
                    except Exception as e:
                        logger.error(f'{e} Error in uploading {filename}')

    # Delete file from local
    assert_file_exists(local_Path, 'tmp.csv.gz')

    logger.info("1010 data processed to staging")


if __name__ == '__main__':

    my_parser = argparse.ArgumentParser(description='Starting transformer decrypt pipeline')
    my_parser.add_argument('--date', metavar='', type=str, help='Date', required=False, default=None)
    args = my_parser.parse_args()
    args_dt = args.date or dt.datetime.now().strftime('%Y%m%d')

    with open('../../config/transformer.yml', 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    table_list = cfg.get('table_list', '')
    bucket = cfg.get('bucket', '')
    s3_staging_path = cfg.get('s3_staging_path', '')
    s3_staging_path_1010 = cfg.get('s3_staging_path_1010', '')
    s3_tmp_path = cfg.get('s3_tmp_path', '')
    s3_archive_path = cfg.get('s3_archive_path', '')
    remote_1010_path = cfg.get('remote_1010_path','')
    remote_Aurus_path = cfg.get('remote_Aurus_path','')
    root_dir = cfg.get('root_dir','')
    local_Path = cfg.get('local_Path','')
    gnupghome = cfg.get('gnupghome','')
    secret_name = cfg.get('secret', '')
    passphrase = Secret(secret_name).get_passphrase()

    # Download & Upload Aurus files from bridg-client-ftp to s3 transformed directory
    process_aurus(args_dt)

    # Download & Upload 1010 files from bridg-client-ftp to s3 transformed directory
    process_1010(args_dt)

