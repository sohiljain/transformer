import boto3
import gnupg
import os, logging
import datetime as dt
import errno
from io import StringIO

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# S3 object
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

# root_dir = '/Users/sjain/PycharmProjects/dg_transformer/src'
root_dir = '/code'
session = boto3.Session()
bucket = 'bridg-client-ftp'
passphrase = 'z7$JP}Q)HC*@9YXY'
gnupghome = f'{root_dir}/gpghome/'
local_Path = f'{root_dir}/Documents/DG'
staging_copy_path = 'dollargeneral/transformed/history_archive'

# Paths
decryption_key_Aurus = f'{root_dir}/gpghome/aurus_decrypt_key.gpg'
decryption_key_1010 = f'{root_dir}/gpghome/1010_decrypt_key.gpg'
remote_1010_path = 'dollargeneral/1010/Historical'
s3_staging_path = 'dollargeneral/transformed/history_staging'
s3_gold_path = 'dollargeneral/transformed/history_gold'
s3_tmp_path = 'dollargeneral/transformed/tmp'

# Configuring gpg decrypter for Aurus & 1010
def gpg_decrytion(decryption_key):
    gpg_decrypt = gnupg.GPG(gnupghome=gnupghome)
    gpg_decrypt.encoding = 'utf-8'
    with open(decryption_key, 'rb') as f:
        key_data = f.read()
    import_result = gpg_decrypt.import_keys(key_data)
    return gpg_decrypt


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


# Download & Upload 1010 files from bridg-client-ftp to s3 transformed directory
def process_1010(date_value=dt.datetime.now().strftime('%Y%m%d')):
    """
    Downloads recursively the given S3 path to the target directory.
    :param client: S3 client to use.
    :param bucket: the name of the bucket to download from
    :param remote_1010_path: The S3 directory to download.
    :param local_Path: the local directory to download the files to.
    """
    gpg_1010 = gpg_decrytion(decryption_key_1010)
    paginator = s3_client.get_paginator('list_objects')

    list_files = ['transactions', 'product', 'product_category', 'organization', 'tenders', 'transaction_item', 'trans_disc_xref', 'discounts']

    # list_files_2 = []

    for folder in list_files:
        for result in paginator.paginate(Bucket=bucket,
                                         Prefix=f'{remote_1010_path}/{folder.title()}/bridg_{folder}_{date_value}'):
            process_1010_files(result, gpg_1010, folder)

    # for folder in list_files_2:
    #     for result in paginator.paginate(Bucket=bucket,
    #                                      Prefix=f'{remote_1010_path}/{folder.title()}/bridg_{folder}_category_{date_value}'):
    #         process_1010_files(result, gpg_1010, f'{folder}_category')

    # Delete file from local
    assert_file_exists(local_Path, 'tmp.csv.gz')

    logger.info("1010 data processed to staging")


def s3_delete_file(s3_path):
    try:
        s3_resource.Bucket(bucket).objects.filter(Prefix=s3_path).delete()
        logger.info(f"Deleted s3://{bucket}/{s3_path}")
    except Exception as e:
        logger.error(f'{e} Cannot delete {s3_path}')


def copy_staging_files(s3_path, folder, filename):
    # Copy object A as object B
    logger.info((f"Copy staging file to archive"))
    try:
        s3_resource.Object(bucket, f'{staging_copy_path}/{folder}/{filename}').copy_from(
            CopySource=f'{bucket}/{s3_path}')
    except Exception as e:
        logger.error(f'{e} Error in copy staging file')
    logger.info((f"Done copy staging file to {staging_copy_path}/{folder}/{filename}"))


def process_1010_files(result, gpg_1010, folder):
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
                s3_upload_file_path = f'''{s3_staging_path}/{folder.lower()}/dt={partition_col}/{filename.replace('psv.gz.pgp', 'psv.gz')}'''
                s3_resource.meta.client.upload_file(Filename='tmp.csv.gz', Bucket=bucket,
                                                    Key=s3_upload_file_path)
                logger.info(f'tmp.csv.gz uploaded to {s3_upload_file_path}')

                # Copying file from staging to archive directory to have a backup
                copy_staging_files(s3_upload_file_path, folder, filename.replace('psv.gz.pgp', 'psv.gz'))
            except Exception as e:
                logger.error(f'{e} Error in uploading {filename}')


if __name__ == '__main__':

    # Download & Upload 1010 files from bridg-client-ftp to s3 transformed directory
    # for datecheck in ['20200901', '20200902', '20200901', '20200902', '20200901', '20200902', '20200901', '20201002', '20201001', '20201002']:
    process_1010('201908')
