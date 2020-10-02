import logging
import boto3
from pyspark.sql import SparkSession

# Paths
bucket = 'bridg-client-ftp'
s3a_bucket = 's3a://xxxxx:yyyyyy@bridg-client-ftp'
s3_staging_path = 'dollargeneral/transformed/staging'
s3_tmp_path = 'dollargeneral/transformed/tmp'
s3_gold_path = 'dollargeneral/transformed/gold'
s3_resource = boto3.resource('s3', aws_access_key_id='xxxxx',
                             aws_secret_access_key='yyyyyy')
s3_client = boto3.client('s3', aws_access_key_id='xxxxx',
                         aws_secret_access_key='yyyyyy')

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create table queries
table_queries = {}
table_queries['transactions'] = """
select b.*
from dg_transactions b"""
table_queries['transactions_partition_col'] = 'datecreated'

table_queries['tenders'] = """
select b.sourcecustomernumber, b.registernumber, b.transactiontimestamp, b.currency,
c.customer_name,
a.sourcetransactionnumber, a.sourceorganizationnumber, a.transactiondate, a.tendercode,
a.tendername, a.tenderamt, a.accountnumbermasked,a.cardusagetype
from dg_tenders a
left join dg_transactions b
    on a.dt=b.dt
    and a.sourcetransactionnumber = b.sourcetransactionnumber
    and a.sourceorganizationnumber = b.sourceorganizationnumber
    and a.transactiondate = b.datecreated
left join dg_aurus c
    on c.store_id = a.sourceorganizationnumber
    and c.pos_register_number=cast(b.registernumber as integer)
    and to_date(left(store_transaction_date_time,10),'M/dd/yyyy') = a.transactiondate
    and substring(c.masked_card_number,-4) = substring(a.accountnumbermasked,-4)
    and substring(c.masked_card_number,1,6) = substring(a.accountnumbermasked,1,6)
    and c.approved_amount = a.tenderamt
"""
table_queries['tenders_partition_col'] = 'transactiondate'

# concat(b.sourcetransactionnumber, '~', d.postransactionnumber , '~' ,d.registernumber) check_id
table_queries['transaction_item'] = """
    select a.*
    from dg_transaction_item a join dg_transactions b
    on a.dt=b.dt
    and a.sourcetransactionnumber = b.sourcetransactionnumber
    and a.sourceorganizationnumber =b.sourceorganizationnumber
    and a.datecreated = b.datecreated
    """
table_queries['transaction_item_partition_col'] = 'datecreated'

table_queries['discounts'] = """
    select
    b.DateCreated, b.SourceTransactionNumber, b.SourceTransactionItemNumber, b.SourceOrganizationNumber, b.InvoiceDate,
    b.ShipDate, b.SourceProductNumber, a.DiscountCode, a.DiscountAmt, c.DiscountType, c.DiscountDescription
    from dg_trans_disc_xref a join dg_transaction_item b
    on  a.dt=b.dt and
        a.SourceTransactionItemNumber = b.SourceTransactionItemNumber
    join dg_transactions d on a.dt=d.dt 
        and b.Sourcetransactionnumber = d.Sourcetransactionnumber
        and b.sourceorganizationnumber =d.sourceorganizationnumber
        and b.datecreated = d.datecreated
    join dg_discounts c on a.dt=c.dt 
        and a.discountcode = c.discountcode"""
table_queries['discounts_partition_col'] = 'datecreated'

# Creating temporary tables for joining data
def create_temptable(table_name, spark):
    table_path = f'{s3a_bucket}/{s3_staging_path}/{table_name}/'
    logger.info(f'Creating table {table_name} on {table_path}')

    if table_name == 'aurus':
        df = spark.read.csv(
            f'{s3a_bucket}/{s3_staging_path}/{table_name}/ADTFF_5_4_4_*',
            sep=',', header=True, nullValue='\\N')

        (df.withColumn("Store_ID", df["Store_ID"].cast("integer"))
         .withColumn("Approved_Amount", df["Approved_Amount"].cast("double"))
         .createOrReplaceTempView('dg_aurus'))
        logger.info("Created Aurus")

    else:
        df = spark.read.csv(table_path, sep='|', header=True, nullValue='\\N')

        if table_name == 'transactions':
            (df.withColumn("SourceOrganizationNumber", df["SourceOrganizationNumber"].cast("integer"))
             .withColumn("DateCreated", df["DateCreated"].cast("date"))
             .withColumn("registernumber", df["registernumber"].cast("integer"))
             .createOrReplaceTempView('dg_transactions'))
            logger.info("Created Transaction")

        if table_name == 'transaction_item':
            (df.withColumn("SourceOrganizationNumber", df["SourceOrganizationNumber"].cast("integer"))
             .withColumn("datecreated", df["datecreated"].cast("date"))
             .createOrReplaceTempView('dg_transaction_item'))
            logger.info("Created Transaction Item")

        if table_name == 'tenders':
            (df.withColumn("SourceOrganizationNumber", df["SourceOrganizationNumber"].cast("integer"))
             .withColumn("transactiondate", df["transactiondate"].cast("date"))
             .withColumn("tenderamt", df["tenderamt"].cast("double"))
             .createOrReplaceTempView('dg_tenders'))
            logger.info("Created Tenders")

    df.createOrReplaceTempView(f'dg_{table_name}')
    logger.info(f'Created temp view for {table_name}')


# Renaming the gold file from part file
def format_gold_file(table, date_value, spark):
    try:
        logger.info(f'Executing query {table_queries.get(table)}')
        df = spark.sql(table_queries.get(table))
        partition_col = table_queries.get(f'{table}_partition_col')

        tmp_path = f"{s3a_bucket}/{s3_tmp_path}/{table}/"
        df.repartition(1, 'dt').write.partitionBy('dt').csv(tmp_path, header=True, compression='gzip',
                                                                              sep='|', emptyValue='', mode='overwrite')
        logger.info(f'{tmp_path} writing done')

    except Exception as e:
        logger.error(f'{e} Error in read write spark file')

    try:
        from datetime import datetime
        datetime_str = date_value
        datetime_object = datetime.strptime(datetime_str, '%Y%m%d')
        dt = datetime_object.strftime("%Y-%m-%d")
        tmp_path_2 = f'{s3_tmp_path}/{table}/dt={date_value}/part'

        logger.info(f'Moving files from {tmp_path_2} to gold')
        for key in get_matching_s3_keys(bucket, prefix=tmp_path_2, suffix='.gz'):
            logger.info(f'{key}')
            copy_source = {
                'Bucket': bucket,
                'Key': key
            }

            s3_gold_temp = f'{s3_gold_path}/{table}/{table}_{date_value}.psv.gz'
            logger.info(f'Moving files to {s3_gold_temp} ')
            if 'transaction_item' in s3_gold_path:
                gold_path = s3_gold_temp.replace('transaction_', 'line_')
            logger.info('In copy')
            s3_resource.meta.client.copy(copy_source, bucket, s3_gold_temp)

        logger.info(f'{tmp_path_2} renamed to {s3_gold_temp}')


    except Exception as e:
        logger.error(f'{e} Unable to rename file')


# Delete the temporary files created during processing on S3
def s3_delete_file(s3_path):
    try:
        s3_resource.Bucket(bucket).objects.filter(Prefix=s3_path).delete()
        logger.info(f"Deleted s3://{bucket}/{s3_path}")
    except Exception as e:
        logger.error(f'{e} Cannot delete {s3_path}')


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


if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()
    logger.info('spark initiated')

    for table in ['trans_disc_xref', 'aurus']:
        create_temptable(table, spark)
        logger.info(f'{table} created')

    for table in ['transactions', 'tenders', 'transaction_item', 'discounts']:
        logger.info(f'Starting {table}')
        create_temptable(table, spark)
        format_gold_file(table, '20200920', spark)
        logger.info(f'Finished {table}')

    # s3_delete_file(s3_tmp_path)
    # logger.info(f"Deleted temporary file path {s3_tmp_path}")
    # s3_delete_file(s3_staging_path)
    # logger.info(f"Deleted staging file path {s3_staging_path}")
