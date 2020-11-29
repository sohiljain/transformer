import logging
import sys
import boto3
from pyspark.sql.functions import *
from utils.utils import get_matching_s3_keys, s3_delete_file, count_check
from utils.config import DgConfig

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


#Creating temporary tables for joining data
def create_temptable(table_name, spark, dg_config):
    table_path = f'{dg_config.bucket}/{dg_config.s3_staging_path_1010}/{table_name}/'
    logger.info(f'Creating table dg_{table_name} on {table_path}')

    if table_name == 'aurus':
        df = spark.read.csv(
            f'{dg_config.bucket}/{dg_config.s3_staging_path}/{table_name}/ADTFF_5_4_4_*',
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


# Renaming the gold file from part file
def format_gold_file(table, spark, dg_config, args_dt):
    try:

        logger.info(f'Executing query {dg_config.table_queries.get(table)}')
        df = spark.sql(dg_config.table_queries.get(table))

        tmp_path = f"{dg_config.bucket}/{dg_config.s3_tmp_path}/{table}/"
        df.repartition(1, 'partition_col').write.partitionBy('partition_col').csv(tmp_path, header=True, compression='gzip',
                                                                              sep='|', emptyValue='', mode='overwrite')
        logger.info(f'{tmp_path} writing done')

    except Exception as e:
        logger.error(f'{e} Error in read write spark file')
        sys.exit(1)

    # Record count validation call
    # Proceed writing to gold only if validation succeeds otherwise call sns_alert
    count_check(spark, dg_config.bucket, dg_config.s3_archive_path, dg_config.s3_tmp_path, table, args_dt)

    try:
        tmp_path_2 = f'{dg_config.s3_tmp_path}/{table}/partition_col'

        logger.info(f'Moving files from {tmp_path_2} to gold')
        for key in get_matching_s3_keys(dg_config.bucket, prefix=tmp_path_2, suffix='.gz'):
            copy_source = {
                'Bucket': dg_config.bucket,
                'Key': key
            }

            dt = key.split('/')[-2].split('=')[1]

            s3_gold_temp = f'{dg_config.s3_gold_path}/{table}/bridg_{table}_{dt}.psv.gz'
            if 'transaction_item' in s3_gold_temp:
                s3_gold_temp = s3_gold_temp.replace('transaction_', 'line_')
            logger.info(f'Moving files to {s3_gold_temp} ')
            s3_delete_file(s3_gold_temp, dg_config.bucket)
            s3_resource.meta.client.copy(copy_source, dg_config.bucket, s3_gold_temp)

        logger.info(f'{tmp_path_2} renamed to {s3_gold_temp}')

    except Exception as e:
        logger.error(f'{e} Unable to rename file')

#TODO add sns alerts
#TODO add comments and copyrithgs
#TODO create README
#TODO add between logic

def process_gold(spark, dg_config: DgConfig, args_dt):
    try:

        spark.sql("set fs.s3a.multiobjectdelete.enable=false")
        logger.info('spark initiated')

        for table in ['aurus', 'trans_disc_xref']:
            create_temptable(table, spark, dg_config)
            logger.info(f'{table} created')

        for table in ['transactions', 'product_category', 'product', 'organization', 'tenders', 'transaction_item', 'discounts']:
            logger.info(f'Starting {table}')
            create_temptable(table, spark, dg_config)
            format_gold_file(table, spark, dg_config, args_dt)
            logger.info(f'Finished {table}')

    finally:
        s3_delete_file(dg_config.s3_tmp_path, dg_config.bucket)
        logger.info(f"Deleted temporary file path {dg_config.s3_tmp_path}")
        s3_delete_file(dg_config.s3_staging_path_1010, dg_config.bucket)
        logger.info(f"Deleted staging file path {dg_config.s3_staging_path_1010}")

        #TODO sns alert

# if __name__ == '__main__':

    # try:
    #     my_parser = argparse.ArgumentParser(description='Starting transformer gold pipeline')
    #     my_parser.add_argument('--date', metavar='', type=str, help='Date', required=False, default=None)
    #     args = my_parser.parse_args()
    #     args_dt = args.date or dt.datetime.now().strftime('%Y%m%d')
    #     with open(f'{root_dir}/config/transformer.yml', 'r') as yml_file:
    #         cfg = yaml.safe_load(yml_file)
    #
    #     logging.info("configuration file passed for = {}".format(args.config))
    #     table_list = cfg.get('table_list', '')
    #     bucket = cfg.get('bucket', '')
    #     s3a_bucket = cfg.get('s3a_bucket','')
    #     s3_staging_path = cfg.get('s3_staging_path','')
    #     s3_staging_path_1010 = cfg.get('s3_staging_path_1010','')
    #     s3_tmp_path = cfg.get('s3_tmp_path','')
    #     s3_gold_path= cfg.get('s3_gold_path','')
    #     s3_archive_path = cfg.get('s3_archive_path','')
    #     table_queries = cfg.get('table_queries', '')
    #     cols = cfg.get('cols', '')

