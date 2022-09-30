# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

import logging
import sys
import boto3
from pyspark.sql.functions import *
from utils.utils import get_matching_s3_keys, s3_delete_file, count_check, send_sns_alert
from utils.config import DgConfig
from datetime import datetime, timedelta
import json

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def format_metadata(brand_id, entity_type, source_count, destination_count, ingestion_date, event_start_time,
                    event_end_time, source, destination, args, event_result, event_notes={}):
    event_type = 'transformer'
    data_category = 'pos'
    event_notes.update({"SOURCE_TYPE": "S3", "DESTINATION_TYPE": "S3", "COMMAND_ARGS": vars(args)})

    return {'BRAND_SID': brand_id,
            'EVENT_TYPE': event_type,
            'DATA_CATEGORY': data_category,
            'INGESTION_DATE': ingestion_date,
            'ENTITY_TYPE': entity_type,
            'SOURCE_COUNT': source_count,
            'DESTINATION_COUNT': destination_count,
            'SOURCE': source,
            'DESTINATION': destination,
            'EVENT_START_TIME': event_start_time,
            'EVENT_END_TIME': event_end_time,
            'EVENT_RESULT': event_result,
            'EVENT_NOTES': json.dumps(event_notes)
            }


def create_temptable(table_name, spark, dg_config):
    """
    Creating temporary tables for joining data
    :param table_name: table name
    :param spark: spark
    :param dg_config: config
    :return: appropriate success/failure message
    """

    if table_name == 'aurus':
        table_path = f'{dg_config.s3a_bucket}/{dg_config.s3_staging_path}/{table_name}/ADTFF_5_4_4_*'
        logger.info(f'Creating table dg_{table_name} on {table_path}')

        df = spark.read.csv(table_path, sep=',', header=True, nullValue='\\N')

        df = (df.withColumn("Store_ID", df["Store_ID"].cast("integer"))
              .withColumn("Approved_Amount", df["Approved_Amount"].cast("double")))

    else:
        table_path = f'{dg_config.s3a_bucket}/{dg_config.s3_staging_path_1010}/{table_name}/'
        logger.info(f'Creating table dg_{table_name} on {table_path}')

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


def format_gold_file(table, spark, dg_config, type, schedule, args_dt):
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

    except Exception as e:
        logger.error(f'{e} Error in reading staging file in spark')
        raise Exception(f'{e} Error in reading staging file in spark')

    if df.count() == 0:
        logger.error("--------------------")
        logger.error(f"Zero Records in File for table: {table}")

    else:
        tmp_path = f"{dg_config.s3a_bucket}/{dg_config.s3_tmp_path}/{table}/"
        df.repartition(1, 'partition_col').write.partitionBy('partition_col').csv(tmp_path, header=True,
                                                                                  compression='gzip', sep='|',
                                                                                  emptyValue='', mode='overwrite',
                                                                                  quote='"', escape='"')
        logger.info(f'{tmp_path} writing done')

        # Record count validation call. Proceed writing to gold only if validation succeeds otherwise call sns_alert
        source_count, destination_count, event_result = count_check(spark, dg_config.s3a_bucket, dg_config.s3_staging_path_1010, dg_config.s3_tmp_path, table, type)

        try:
            tmp_path_2 = f'{dg_config.s3_tmp_path}/{table}/partition_col'

            logger.info(f'Moving files from {tmp_path_2} to gold')
            for key in get_matching_s3_keys(dg_config.bucket, prefix=tmp_path_2, suffix='.gz'):
                copy_source = {
                    'Bucket': dg_config.bucket,
                    'Key': key
                }

                dt = key.split('/')[-2].split('=')[1]
                tommorrow_date = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
                yesterday_date = (datetime.strptime(dt, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

                if schedule == "weekly":
                    s3_gold_temp = f'{dg_config.s3_gold_path}/{table}/bridg_{table}_{tommorrow_date}_{dt}_{schedule}.psv.gz'
                else:
                    s3_gold_temp = f'{dg_config.s3_gold_path}/{table}/bridg_{table}_{yesterday_date}_{dt}_{schedule}.psv.gz'
                if 'transaction_item' in s3_gold_temp:
                    s3_gold_temp = s3_gold_temp.replace('transaction_', 'line_')
                logger.info(f'Moving files to {s3_gold_temp} ')
                s3_delete_file(s3_gold_temp, dg_config.bucket)
                s3_resource.meta.client.copy(copy_source, dg_config.bucket, s3_gold_temp)

            logger.info(f'{tmp_path_2} renamed to {s3_gold_temp}')
            return source_count, destination_count, event_result

        except Exception as e:
            logger.error(f'{e} Unable to rename file')
            raise Exception(f'{e} Unable to rename file')


def process_gold(spark, dg_config: DgConfig, args):
    """
    Starting gold data processing
    :param spark: spark context
    :param dg_config: config
    :return: appropriate success/failure message
    """

    #Initializing variables
    all_tables_metadata = []
    type = args.type
    schedule = args.schedule
    args_dt = args.date
    event_start_time = datetime.utcnow()
    source_count, destination_count = None, None

    try:
        spark.sql("set fs.s3a.multiobjectdelete.enable=false")
        spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
        logger.info('spark initiated')

        # clean temp files if already present
        s3_delete_file(dg_config.s3_tmp_path, dg_config.bucket)
        logger.info(f"Deleted temporary file path {dg_config.s3_tmp_path}")

        for table in ['aurus', 'trans_disc_xref']:
            create_temptable(table, spark, dg_config)
            logger.info(f'{table} created')

        for table in ['transactions', 'product_category', 'product', 'organization', 'tenders', 'transaction_item', 'discounts']:
            #Initializing source_count and destination_count values before starting each table
            source_count, destination_count = None, None
            logger.info(f'Starting {table}')

            create_temptable(table, spark, dg_config)

            event_start_time = datetime.utcnow()
            source_count, destination_count, event_result = format_gold_file(table, spark, dg_config, type, schedule, args_dt)
            event_end_time = datetime.utcnow()

            if event_result == 'FAILURE':
                raise Exception(f"Count check failure for {table}. Transformed record count : {destination_count} Staging row count : {source_count}")

            table_metadata = format_metadata(
                brand_id=type,
                entity_type=table,
                ingestion_date=args_dt,
                source_count=source_count,
                destination_count=destination_count,
                source=dg_config.remote_1010_path,
                event_start_time=event_start_time,
                event_end_time=event_end_time,
                destination=dg_config.s3_gold_path,
                args = args,
                event_result=event_result
            )
            all_tables_metadata.append(table_metadata)
            logger.info(f'Finished {table}')
            logger.info('---------------------------')

    except Exception as e:
        table_metadata = format_metadata(
            brand_id=type,
            entity_type=table or "",
            ingestion_date=args_dt,
            source_count= source_count,
            destination_count= destination_count,
            source=dg_config.remote_1010_path,
            event_start_time=event_start_time,
            event_end_time=datetime.utcnow(),
            destination=dg_config.s3_gold_path,
            args=args,
            event_result='FAILURE',
            event_notes={'FAILURE_REASON': e.desc}
        )
        all_tables_metadata.append(table_metadata)

        send_sns_alert(f"{type} Transformer: EMR processing failed", e)
        logger.error(f"{type} Transformer: EMR processing failed {e}", exc_info=True)
        raise Exception(f"{type} Transformer: EMR processing failed {e}")

    finally:
        filepath = f"{dg_config.s3a_bucket}/{dg_config.metadata_path}{args_dt}_{datetime.utcnow()}"
        tmp_path = f"{dg_config.s3a_bucket}/{dg_config.s3_tmp_path}/{table}/"
        df = spark.createDataFrame(all_tables_metadata)
        df = df.select(["BRAND_SID", "EVENT_TYPE", "DATA_CATEGORY", "INGESTION_DATE", "ENTITY_TYPE", "SOURCE_COUNT", "DESTINATION_COUNT", "SOURCE", "DESTINATION", "EVENT_START_TIME", "EVENT_END_TIME", "EVENT_RESULT", "EVENT_NOTES"])
        df.repartition(1).write.csv(filepath, header=False, sep='\001')
        logger.info(f'{filepath} writing done')
        logger.info(f"Metadata writing done on {dg_config.s3a_bucket}/{dg_config.metadata_path}")
        s3_delete_file(dg_config.s3_tmp_path, dg_config.bucket)
        logger.info(f"Deleted temporary file path {dg_config.s3_tmp_path}")
        s3_delete_file(dg_config.s3_staging_path_1010, dg_config.bucket)
        logger.info(f"Deleted staging file path {dg_config.s3_staging_path_1010}")
