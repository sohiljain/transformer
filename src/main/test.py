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
 
 import pyspark
from pyspark.sql import SparkSession
import os, logging

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

s3a_bucket = 's3a://xxx:yyy@bridg-client-ftp'
s3_staging_path = 'dollargeneral/transformed/staging'
s3_gold_path = 'dollargeneral/transformed/gold'
staging_copy_path = 'dollargeneral/transformed/archive'

for table in ['transactions', 'product_category', 'product', 'organization', 'tenders', 'discounts', 'transaction_item']:
    df_archive = spark.read.csv(f'{s3a_bucket}/{staging_copy_path}/{table}/bridg_*',sep='|', header=True, nullValue='\\N')
    table = table.replace('transaction_', 'line_').replace('trans_disc_xref','discounts')
    df_gold = spark.read.csv(f'{s3a_bucket}/{s3_gold_path}/{table}/bridg_*',sep='|', header=True, nullValue='\\N')
    print(f'{table} test begins')
    
    if table == 'transaction_item':
        print(f'''Transactiontimestamp nulls - {df_gold.where("transactiontimestamp='' or transactiontimestamp".count())} ''')

    if table == 'tenders':
        print(f'''Customer_name counts - {df_gold.select("customer_name").distinct().count() } ''')

    if table in ['line_item', 'transactions', 'tenders', 'discounts']:
        print(f'''CheckId nulls - {df_gold.where("check_id='' or check_id is null").count()}''')
        
    print(f'Gold - {df_gold.count()}')
    print(f'Archive - {df_archive.count()}')
    print()        
    
