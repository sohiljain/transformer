# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

import boto3
import os, logging, sys, json
import datetime as dt
from utils.utils import copy_staging_files, assert_file_exists, gpg_decrytion, send_sns_alert, s3_delete_file
from utils.config import DgConfig

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# S3 object
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')


def process_1010(dg_config, root_dir, schedule, date_value=dt.datetime.now().strftime('%Y%m%d')):
    """
    Downloads recursively the given S3 path to the target directory.
    Download & Upload 1010 files from bridg-client-ftp to s3 transformed staging/archive directory
    :param dg_config: config
    :param root_dir: root directory for decryption key
    :param date_value: date of file to be processed
    :return: appropriate success/failure message
    """
    gpg_1010 = gpg_decrytion('1010_decrypt_key.gpg', root_dir, dg_config.bucket, dg_config.gnupghome)
    paginator = s3_client.get_paginator('list_objects')

    list_files = ['transactions', 'product', 'product_category', 'organization', 'tenders', 'transaction_item',
                  'trans_disc_xref', 'discounts']

    list_files_weekly = ['product', 'product_category', 'organization']

    for folder in list_files:
        prefix = f'{dg_config.remote_1010_path}/{folder.title()}/bridg_{folder}_{date_value}'
        logger.info(f'Downloading from {prefix}')

        try:
            for result in paginator.paginate(Bucket=dg_config.bucket,Prefix=prefix):
                # Download each file individually
                for content in result['Contents']:

                    filename = content['Key'].split('/')[-1]

                    try:
                        # Skip paths ending in /
                        if not content['Key'].endswith('/'):
                            local_file_absolute_path = f'{dg_config.local_Path}/{filename}'

                            # Make sure file does not exist
                            assert_file_exists(dg_config.local_Path, filename)

                            # Download file to local path
                            s3_client.download_file(dg_config.bucket, content['Key'], local_file_absolute_path)
                            logger.info(f'{local_file_absolute_path} downloaded')

                            # Decrypt file
                            with open(local_file_absolute_path, 'rb') as f:
                                gpg_1010.decrypt_file(f, passphrase=dg_config.passphrase, output="tmp.csv.gz")
                            logger.info(f'{local_file_absolute_path} decrypted to tmp.csv.gz')
                            os.remove(local_file_absolute_path)

                            # Upload file to staging by date partition
                            partition_col = filename.split('_')[-1][0:8]
                            s3_upload_file_path = f'''{dg_config.s3_staging_path_1010}/{folder.lower()}/dt={partition_col}/{filename.replace('psv.gz.pgp', 'psv.gz')}'''
                            s3_resource.meta.client.upload_file(Filename='tmp.csv.gz', Bucket=dg_config.bucket,
                                                                Key=s3_upload_file_path)
                            logger.info(f'tmp.csv.gz uploaded to {s3_upload_file_path}')

                            # Copying file from staging to archive directory to have a backup
                            copy_staging_files(s3_upload_file_path, folder, filename.replace('psv.gz.pgp', 'psv.gz'),
                                               dg_config.bucket, dg_config.s3_archive_path)

                            #Copy the product, org files to weekly path
                            if {folder} in list_files_weekly and schedule == 'daily':
                                s3_upload_weekly_file_path = f'''{dg_config.s3_staging_path_weekly_1010}/{folder.lower()}/dt={partition_col}/{filename.replace('psv.gz.pgp', 'psv.gz')}'''
                                copy_staging_files(s3_upload_file_path, folder, filename.replace('psv.gz.pgp', 'psv.gz'),
                                               dg_config.bucket, s3_upload_weekly_file_path)

                                s3_upload_weekly_archive_file_path = f'''{dg_config.s3_archive_path_weekly}/{folder.lower()}/dt={partition_col}/{filename.replace('psv.gz.pgp', 'psv.gz')}'''
                                copy_staging_files(s3_upload_file_path, folder, filename.replace('psv.gz.pgp', 'psv.gz'),
                                               dg_config.bucket, s3_upload_weekly_archive_file_path)



                    except Exception as e:
                        raise Exception(f"Error in PGP-Decrypt 1010\n {e}")

        except Exception as e:
            raise Exception(f"S3 Object not found {prefix}\n {e}")

    # Delete file from local
    assert_file_exists(dg_config.local_Path, 'tmp.csv.gz')
    logger.info("1010 data processed to staging")


def process_aurus(dg_config, root_dir, date_value=dt.datetime.now().strftime('%Y%m%d')):
    """
    Download & Upload Aurus files from bridg-client-ftp to s3 transformed staging/archive directory
    :param dg_config: config
    :param root_dir: root directory
    :param date_value: date of file to be processed
    :return: appropriate success/failure message
    """
    gpg_aurus = gpg_decrytion('aurus_decrypt_key.gpg', root_dir, dg_config.bucket, dg_config.gnupghome)
    paginator = s3_client.get_paginator('list_objects')
    prefix = f'{dg_config.remote_Aurus_path}/ADTFF_5_4_4_{date_value}'
    logger.info(f'Downloading from {prefix}')

    try:
        # Iterate over directory
        for result in paginator.paginate(Bucket=dg_config.bucket, Prefix=prefix):
            for content in result.get('Contents', []):
                filename = content['Key'].split('/')[-1]
                folder = content['Key'].split('/')[-3]

                try:
                    # Skip paths ending in /
                    if not content['Key'].endswith('/'):
                        local_file_absolute_path = f'{dg_config.local_Path}/{filename}'

                        s3_upload_file_path = f'''{dg_config.s3_staging_path}/{folder.lower()}/{filename.replace('.pgp', '')}'''

                        # Make sure directories exist and file doesnot exist
                        assert_file_exists(dg_config.local_Path, filename)

                        # Aurus - Download file to local path
                        s3_client.download_file(dg_config.bucket, content['Key'], local_file_absolute_path)
                        logger.info(f'{local_file_absolute_path} downloaded')

                        # Decrypt aurus file and Upload to tranformed s3 directory
                        with open(local_file_absolute_path, 'rb') as f:
                            gpg_aurus.decrypt_file(f, passphrase=dg_config.passphrase, output="tmp.csv")

                        logger.info(f'{local_file_absolute_path} decrypted to tmp.csv')

                        # Upload file to s3
                        s3_resource.meta.client.upload_file(Filename='tmp.csv', Bucket=dg_config.bucket,
                                                            Key=s3_upload_file_path)
                        logger.info(f'tmp.csv uploaded to {s3_upload_file_path}')

                except Exception as e:
                    raise Exception(f"Error in PGP-Decrypt Aurus \n {e}")

    except Exception as e:
        raise Exception(f"S3 Object not found {prefix}\n {e}")


def pgp_decrypt(dg_config: DgConfig, args_dt, root_dir, env, type, schedule):
    """
    Serially download & upload Aurus/1010 files from bridg-client-ftp to s3 transformed directory
    1. Download raw Aurus/1010 files from the ftp bucket according to args_dt date/date_range provided
    2. PGP-Decrypt the file using python library
    3. Upload the decrypted file to s3 transformed staging directory for gold processing
    4. Upload the decrypted file to s3 transformed archive directory for backup
    :param dg_config: config
    :param args_dt: date/date_range of file to be processed
    :param root_dir: root directory
    :return: sns alert to start emr job
    """

    try:
        # Start job according to date_pattern
        # 1. dt1,dt2,dt3... process multiple dates by splitting on comma
        # 2. dt1:dt2 - process all dates between dt1 and dt2 inclusive on both sides
        # 3. dt - process a single date

        # clean staging files if already present
        s3_delete_file(dg_config.s3_staging_path_1010, dg_config.bucket)
        logger.info(f"Deleted staging file path {dg_config.s3_staging_path_1010}")

        # pattern-1
        if ',' in args_dt:
            for date_arg in args_dt.split(','):
                logger.info(f'Starting process for date - {args_dt}')
                if (date_arg > dg_config.aurus_start_date):
                    process_aurus(dg_config, root_dir, date_arg)
                process_1010(dg_config, root_dir, schedule, date_arg)

        # pattern-2
        elif ':' in args_dt:
            start_date = dt.datetime.strptime(args_dt.split(':')[0], '%Y%m%d')
            end_date = dt.datetime.strptime(args_dt.split(':')[1], '%Y%m%d')
            delta = end_date - start_date
            for i in range(delta.days + 1):  # Adding 1 to include end_date
                logger.info(f'Starting process for date - {start_date}')
                date_arg = (str(start_date)).split(' ')[0].replace('-', '')
                if (date_arg > dg_config.aurus_start_date):
                    process_aurus(dg_config, root_dir, date_arg)
                process_1010(dg_config, root_dir, schedule, date_arg)
                start_date += dt.timedelta(days=1)

        # pattern-3
        else:
            logger.info(f'Starting process for date - {args_dt}')
            if (args_dt > dg_config.aurus_start_date):
                process_aurus(dg_config, root_dir, args_dt)
            process_1010(dg_config, root_dir, schedule, args_dt)

        msg = {"Action": "Start EMR Process Gold", "EnvDetail": env, "Type" : type, "Schedule": schedule}
        json_msg = json.dumps(msg)
        logger.info(f"Sending message - {json_msg} to SNS")

        # send sns alert to start emr job
        send_sns_alert(subject="batch_pgp_decrypt",
                       message=json_msg)

    except Exception as e:
        # send_sns_alert(f"{type} Transformer: PGP Decrypt failed", e)
        logger.error(f"{type} Transformer: PGP Decrypt failed {e}", exc_info=True)
        s3_delete_file(dg_config.s3_staging_path_1010, dg_config.bucket)
        logger.info(f"Cleaned up staging file path {dg_config.s3_staging_path_1010}")
        raise Exception(f"{type} Transformer: PGP Decrypt failed {e}")
