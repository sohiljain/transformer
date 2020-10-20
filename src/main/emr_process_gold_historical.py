import logging
import sys
import boto3
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Paths
bucket = 'bridg-client-ftp'
s3a_bucket = 's3://bridg-client-ftp'
s3_staging_path = 'dollargeneral/transformed/history_staging'
s3_staging_path_aurus = 'dollargeneral/transformed/staging'
s3_tmp_path = 'dollargeneral/transformed/tmp'
s3_gold_path = 'dollargeneral/transformed/history_gold'
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

# create logger
logging.basicConfig(format='%(name)s:%(levelname)s:%(asctime)s:%(lineno)d: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create table queries
table_queries = {}
table_queries['transactions'] = """
select dgtrans.*,
concat(dgtrans.sourcetransactionnumber, '~', dgtrans.postransactionnumber , '~' ,dgtrans.registernumber) check_id,
dt as partition_col
from dg_transactions dgtrans
"""

table_queries['tenders'] = """
with dgaur as (select store_id, 
    store_id, 
    to_date(left(store_transaction_date_time,10),'M/dd/yyyy') dgaur_date,
    masked_card_number, 
    approved_amount,
    pos_register_number, 
    min(customer_name) as customer_name
 from dg_aurus group by 1,2,3,4,5,6)
select dgtrans.sourcecustomernumber, dgtrans.registernumber, dgtrans.transactiontimestamp, dgtrans.currency,
dgaur.customer_name, concat(dgtndrs.sourcetransactionnumber, '~', dgtrans.postransactionnumber , '~' ,dgtrans.registernumber) check_id,
dgtndrs.sourcetransactionnumber, dgtndrs.sourceorganizationnumber, dgtndrs.transactiondate, dgtndrs.tendercode,
dgtndrs.tendername, dgtndrs.tenderamt, dgtndrs.accountnumbermasked,dgtndrs.cardusagetype, dgtndrs.dt,
dgtndrs.dt as partition_col
from dg_tenders dgtndrs
join dg_transactions dgtrans
    on dgtndrs.dt=dgtrans.dt
    and dgtndrs.sourcetransactionnumber = dgtrans.sourcetransactionnumber
    and dgtndrs.sourceorganizationnumber = dgtrans.sourceorganizationnumber
    and dgtndrs.transactiondate = dgtrans.datecreated
left join dgaur
    on dgaur.store_id = dgtndrs.sourceorganizationnumber
    and cast(dgaur.pos_register_number as integer)=cast(dgtrans.registernumber as integer)
    and dgaur.dgaur_date = dgtndrs.transactiondate
    and substring(dgaur.masked_card_number,-4) = substring(dgtndrs.accountnumbermasked,-4)
    and substring(dgaur.masked_card_number,1,6) = substring(dgtndrs.accountnumbermasked,1,6)
    and dgaur.approved_amount = dgtndrs.tenderamt
"""

table_queries['transaction_item'] = """
    select dgtrnitem.*, concat(dgtrnitem.sourcetransactionnumber, '~', dgtrans.postransactionnumber , '~' ,dgtrans.registernumber) check_id,
    dgtrans.transactiontimestamp, dgtrnitem.dt as partition_col
    from dg_transaction_item dgtrnitem join dg_transactions dgtrans
    on dgtrnitem.dt=dgtrans.dt
    and dgtrnitem.sourcetransactionnumber = dgtrans.sourcetransactionnumber
    and dgtrnitem.sourceorganizationnumber =dgtrans.sourceorganizationnumber
    and dgtrnitem.datecreated = dgtrans.datecreated
    """

table_queries['discounts'] = """
    with dgdisc as (select distinct discountcode, DiscountDescription, DiscountType, dt from dg_discounts)
    select
    dgtrnitem.DateCreated, dgtrnitem.SourceTransactionNumber, dgtrnitem.SourceTransactionItemNumber, dgtrnitem.SourceOrganizationNumber, dgtrnitem.InvoiceDate,
    dgtrnitem.ShipDate, dgtrnitem.SourceProductNumber, dptrnxref.DiscountCode, dptrnxref.DiscountAmt, dgdisc.DiscountType, dgdisc.DiscountDescription,
    concat(dgtrnitem.sourcetransactionnumber, '~', dgtrans.postransactionnumber , '~' ,dgtrans.registernumber) check_id, dptrnxref.dt,
    dgtrnitem.dt as partition_col
    from dg_trans_disc_xref dptrnxref join dg_transaction_item dgtrnitem
    on  dptrnxref.dt=dgtrnitem.dt and
        dptrnxref.SourceTransactionItemNumber = dgtrnitem.SourceTransactionItemNumber
    join dg_transactions dgtrans on dptrnxref.dt=dgtrans.dt 
        and dgtrnitem.Sourcetransactionnumber = dgtrans.Sourcetransactionnumber
        and dgtrnitem.sourceorganizationnumber = dgtrans.sourceorganizationnumber
        and dgtrnitem.datecreated = dgtrans.datecreated
    join dgdisc on dptrnxref.dt=dgdisc.dt 
        and dptrnxref.discountcode = dgdisc.discountcode"""

table_queries['organization'] = """
select dgorg.*, dt as partition_col
from dg_organization dgorg"""
table_queries['organization_partition_col'] = 'datecreated'

table_queries['product_category'] = """
select dgprodcatg.*, dt as partition_col
from dg_product_category dgprodcatg"""
table_queries['product_category_partition_col'] = 'datecreated'

table_queries['product'] = """
select dgprod.*, dgprodcatg.Name as Sourceproductcategoryname, dgprod.dt as partition_col
from dg_product dgprod join dg_product_category dgprodcatg 
on dgprod.dt=dgprodcatg.dt
and dgprod.Sourceproductcategorynumber = dgprodcatg.Sourcecategorynumber
and dgprod.datecreated= dgprodcatg.datecreated"""
table_queries['product_partition_col'] = 'datecreated'


# Creating temporary tables for joining data
def create_temptable(table_name, spark):
    table_path = f'{s3a_bucket}/{s3_staging_path}/{table_name}/'
    logger.info(f'Creating table dg_{table_name} on {table_path}')

    if table_name == 'aurus':
        df = spark.read.csv(
            f'{s3a_bucket}/{s3_staging_path_aurus}/{table_name}/ADTFF_5_4_4_*',
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
def format_gold_file(table, spark):
    try:
        # logger.info(f'Executing query {table_queries.get(table)}')
        df = spark.sql(table_queries.get(table))

        tmp_path = f"{s3a_bucket}/{s3_tmp_path}/{table}/"
        df.repartition(1, 'partition_col').write.partitionBy('partition_col').csv(tmp_path, header=True, compression='gzip',
                                                                              sep='|', emptyValue='', mode='overwrite')
        logger.info(f'{tmp_path} writing done')

    except Exception as e:
        logger.error(f'{e} Error in read write spark file')
        sys.exit(1)

    try:
        tmp_path_2 = f'{s3_tmp_path}/{table}/partition_col'

        # logger.info(f'Moving files from {tmp_path_2} to gold')
        for key in get_matching_s3_keys(bucket, prefix=tmp_path_2, suffix='.gz'):
            copy_source = {
                'Bucket': bucket,
                'Key': key
            }

            dt = key.split('/')[-2].split('=')[1]
            # dt_part = key.split('/')[-2].split('=')[1]
            # date_time = datetime.strptime(dt_part, "%Y-%m-%d").date()
            # dt = date_time.strftime("%Y%m%d")
            #
            s3_gold_temp = f'{s3_gold_path}/{table}/bridg_{table}_{dt}.psv.gz'
            if 'transaction_item' in s3_gold_temp:
                s3_gold_temp = s3_gold_temp.replace('transaction_', 'line_')
            # logger.info(f'Moving files to {s3_gold_temp} ')
            s3_delete_file(s3_gold_temp)
            s3_resource.meta.client.copy(copy_source, bucket, s3_gold_temp)

        # logger.info(f'{tmp_path_2} renamed to {s3_gold_temp}')

    except Exception as e:
        logger.error(f'{e} Unable to rename file')


# Delete the temporary files created during processing on S3
def s3_delete_file(s3_path):
    try:
        s3_resource.Bucket(bucket).objects.filter(Prefix=s3_path).delete()
        # logger.info(f"Deleted s3://{bucket}/{s3_path}")
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

    try:
        spark = SparkSession.builder.getOrCreate()
        spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
        logger.info('spark initiated')

        for table in ['aurus', 'trans_disc_xref']:
        # for table in ['aurus', 'trans_disc_xref', 'transactions', 'tenders', 'transaction_item']:
            create_temptable(table, spark)
            logger.info(f'{table} created')

        for table in ['transactions', 'product_category', 'product', 'organization', 'tenders', 'transaction_item', 'discounts']:
        # for table in ['discounts']:
            logger.info(f'Starting {table}')
            create_temptable(table, spark)
            format_gold_file(table, spark)
            logger.info(f'Finished {table}')

    finally:
        s3_delete_file(s3_tmp_path)
        logger.info(f"Deleted temporary file path {s3_tmp_path}")
        s3_delete_file(s3_staging_path)
        logger.info(f"Deleted staging file path {s3_staging_path}")


    cols = {}
    cols['organization'] = ['Sourceorganizationnumberkey',
                            'Name',
                            'Status',
                            'Type',
                            'Subtype',
                            'Parentsourceorganizationnumber',
                            'Country',
                            'State',
                            'City',
                            'Addr_line_1',
                            'Addr_line_2',
                            'Zip',
                            'Excludeascloseststore',
                            'Datecreated',
                            'Customattributes',
                            'dt']

    cols['discounts'] = ['DateCreated',
                         'SourceTransactionNumber',
                         'SourceTransactionItemNumber',
                         'SourceOrganizationNumber',
                         'InvoiceDate',
                         'ShipDate',
                         'SourceProductNumber',
                         'DiscountCode',
                         'DiscountAmt',
                         'DiscountType',
                         'DiscountDescription',
                         'check_id',
                         'dt']

    cols['product_category'] = ['Sourcecategorynumber',
                                'Name',
                                'Sourceparentcategorynumber',
                                'Datecreated',
                                'Customattributes',
                                'dt']

    cols['product'] = ['Sourceproductnumber',
                       'Name',
                       'Description',
                       'Producturl',
                       'Imageurl',
                       'Parentproductnumber',
                       'BrandName',
                       'Msrp',
                       'Listprice',
                       'Saleprice',
                       'Salecondition',
                       'Availability',
                       'Availableqty',
                       'Recostatus',
                       'Size',
                       'Color',
                       'Sourceproductcategorynumber',
                       'Datecreated',
                       'Customattributes',
                       'dt',
                       'Sourceproductcategoryname']

    cols['transactions'] = ['sourcetransactionnumber',
                            'SourceOrganizationNumber',
                            'Total',
                            'Currency',
                            'Discount',
                            'Tax',
                            'Type',
                            'Transactiontimestamp',
                            'Sourcecustomernumber',
                            'DateCreated',
                            'CustomAttributes',
                            'Promoamt',
                            'Couponamt',
                            'Posdiscamt',
                            'Manufacturercouponamt',
                            'registernumber',
                            'AccountNumberMasked',
                            'postransactionnumber',
                            'dt',
                            'check_id']

    cols['tenders'] = ['sourcecustomernumber',
                       'registernumber',
                       'transactiontimestamp',
                       'currency',
                       'customer_name',
                       'check_id',
                       'sourcetransactionnumber',
                       'sourceorganizationnumber',
                       'transactiondate',
                       'tendercode',
                       'tendername',
                       'tenderamt',
                       'accountnumbermasked',
                       'cardusagetype',
                       'dt']

    cols['line_item'] = ['Sourcetransactionitemnumber',
                         'Sourcetransactionnumber',
                         'SourceOrganizationNumber',
                         'Type',
                         'Subtype',
                         'Invoicedate',
                         'Shipdate',
                         'Sourceproductnumber',
                         'Quantity',
                         'Weight',
                         'Volume',
                         'Listprice',
                         'Currency',
                         'Salesrevenue',
                         'Discount',
                         'Costbasis',
                         'Tax',
                         'Shippingrevenue',
                         'Shippingcost',
                         'Shippingdiscount',
                         'Otherrevenue',
                         'Othercosts',
                         'datecreated',
                         'dt',
                         'check_id',
                         'transactiontimestamp']

    s3_gold_path = 'dollargeneral/transformed/history_gold'
    staging_copy_path = 'dollargeneral/transformed/history_archive'

    for table in ['transactions', 'product_category', 'product', 'organization', 'tenders', 'trans_disc_xref', 'transaction_item']:
    # for table in ['trans_disc_xref', 'transaction_item']:
        dt='201908'
        df_archive = spark.read.csv(f'{s3a_bucket}/{staging_copy_path}/{table}/bridg_{table}_{dt}*', sep='|', header=True,
                                    nullValue='\\N')
        table = table.replace('transaction_', 'line_').replace('trans_disc_xref', 'discounts')
        df_gold = spark.read.csv(f'{s3a_bucket}/{s3_gold_path}/{table}/bridg_{table}_{dt}*', sep='|', header=True, nullValue='\\N')
        print(f'{table} test begins')

        if table == 'line_item':
            print(
                f'''Transactiontimestamp nulls - {df_gold.where("transactiontimestamp='' or transactiontimestamp is null").count()} ''')

        if table == 'tenders':
            print(f'''Customer_name counts - {df_gold.select("customer_name").distinct().count()} ''')

        if table in ['line_item', 'transactions', 'tenders', 'discounts']:
            print(f'''CheckId nulls - {df_gold.where("check_id='' or check_id is null").count()}''')

        print(f'Gold - {df_gold.count()}')
        print(f'Archive - {df_archive.count()}')
        print()
