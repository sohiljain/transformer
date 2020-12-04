# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

import logging
import sys
import boto3
from pyspark.sql.functions import *
from utils.utils import get_matching_s3_keys, s3_delete_file, count_check, send_sns_alert
from utils.config import DgConfig

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_temptable(table_name, spark, dg_config):
    """
    Creating temporary tables for joining data
    :param table_name: table name
    :param spark: spark
    :param dg_config: config
    :return: appropriate success/failure message
    """
    table_path = f'{dg_config.s3a_bucket}/{dg_config.s3_staging_path_1010}/{table_name}/'
    logger.info(f'Creating table dg_{table_name} on {table_path}')

    if table_name == 'aurus':
        df = spark.read.csv(
            f'{dg_config.s3a_bucket}/{dg_config.s3_staging_path}/{table_name}/ADTFF_5_4_4_*',
            sep=',', header=True, nullValue='\\N')

        df = (df.withColumn("Store_ID", df["Store_ID"].cast("integer"))
              .withColumn("Approved_Amount", df["Approved_Amount"].cast("double")))

    else:
        df = spark.read.csv(table_path, sep='|', header=True, nullValue='\\N')

        if table_name == 'transactions':
            df = (df.withColumn("SourceOrganizationNumber", df["SourceOrganizationNumber"].cast("integer"))
             .withColumn("DateCreated", df["DateCreated"].cast("date"))
             .withColumn("sourcetransactionnumber", coalesce(df["sourcetransactionnumber"], lit("")))
             .withColumn("postransactionnumber", coalesce(df["postransactionnumber"], lit("")))
             .withColumn("registernumber", coalesce(df["registernumber"], lit("")))
            )

        if table_name == 'transaction_item':
            df = (df.withColumn("SourceOrganizationNumber", df["SourceOrganizationNumber"].cast("integer"))
             .withColumn("datecreated", df["datecreated"].cast("date")))

        if table_name == 'tenders':
            df = (df.withColumn("SourceOrganizationNumber", df["SourceOrganizationNumber"].cast("integer"))
             .withColumn("transactiondate", df["transactiondate"].cast("date"))
             .withColumn("tenderamt", df["tenderamt"].cast("double")))

    df.createOrReplaceTempView(f'dg_{table_name}')
    logger.info(f'Created temp view for dg_{table_name}')


def format_gold_file(table, spark, dg_config):
    """
    Renaming the gold file from part file
    :param table: table name
    :param spark: spark
    :param dg_config: config
    :return: appropriate success/failure message
    """
    try:
        logger.info(f'Executing query {dg_config.table_queries.get(table)}')
        df = spark.sql(dg_config.table_queries.get(table))

        tmp_path = f"{dg_config.s3a_bucket}/{dg_config.s3_tmp_path}/{table}/"
        df.repartition(1, 'partition_col').write.partitionBy('partition_col').csv(tmp_path, header=True, compression='gzip',
                                                                              sep='|', emptyValue='', mode='overwrite')
        logger.info(f'{tmp_path} writing done')

    except Exception as e:
        logger.error(f'{e} Error in read write spark file')
        sys.exit(1)

    # Record count validation call
    # Proceed writing to gold only if validation succeeds otherwise call sns_alert
    count_check(spark, dg_config.s3a_bucket, dg_config.s3_staging_path_1010, dg_config.s3_tmp_path, table)

    try:
        tmp_path_2 = f'{dg_config.s3_tmp_path}/{table}/partition_col'

        logger.info(f'Moving files from {tmp_path_2} to gold')
        for key in get_matching_s3_keys(dg_config.bucket, prefix=tmp_path_2, suffix='.gz'):
            copy_source = {
                'Bucket': dg_config.bucket,
                'Key': key
            }

            dt = key.split('/')[-2].split('=')[1]

            #TODO s3_gold_temp = f'{dg_config.s3_gold_path}/{table}/bridg_{table}_{dt}_{key}.psv.gz' - To make mulitple part files and remove repartition - Sid
            s3_gold_temp = f'{dg_config.s3_gold_path}/{table}/bridg_{table}_{dt}.psv.gz'
            if 'transaction_item' in s3_gold_temp:
                s3_gold_temp = s3_gold_temp.replace('transaction_', 'line_')
            logger.info(f'Moving files to {s3_gold_temp} ')
            s3_delete_file(s3_gold_temp, dg_config.bucket)
            s3_resource.meta.client.copy(copy_source, dg_config.bucket, s3_gold_temp)

        logger.info(f'{tmp_path_2} renamed to {s3_gold_temp}')

    except Exception as e:
        logger.error(f'{e} Unable to rename file')


def process_gold(spark, dg_config: DgConfig):
    """
    Starting gold data processing
    :param spark: spark context
    :param dg_config: config
    :return: appropriate success/failure message
    """
    try:
        spark.sql("set fs.s3a.multiobjectdelete.enable=false")
        logger.info('spark initiated')

        for table in ['aurus', 'trans_disc_xref']:
            create_temptable(table, spark, dg_config)
            logger.info(f'{table} created')

        for table in ['transactions', 'product_category', 'product', 'organization', 'tenders', 'transaction_item', 'discounts']:
            logger.info(f'Starting {table}')
            create_temptable(table, spark, dg_config)
            format_gold_file(table, spark, dg_config)
            logger.info(f'Finished {table}')

    except Exception as e:
        # send_sns_alert("DG Transformer: EMR processing failed", e)
        logger.error(f"DG Transformer: EMR processing failed {e}", exc_info=True)
        raise Exception(f"DG Transformer: EMR processing failed {e}")

    finally:
        s3_delete_file(dg_config.s3_tmp_path, dg_config.bucket)
        logger.info(f"Deleted temporary file path {dg_config.s3_tmp_path}")
        s3_delete_file(dg_config.s3_staging_path_1010, dg_config.bucket)
        logger.info(f"Deleted staging file path {dg_config.s3_staging_path_1010}")

