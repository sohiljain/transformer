CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_transaction_history_gold (
SourceTransactionNumer string,
SourceOrganizationNumber string,
Total string,
Currency string,
Discount string,
Tax string,
Type string,
TransactionTimeStamp string,
SourceCustomerNumber string,
DateCreated string,
Custom_Attributes string,
PromoAmt string,
CouponAmt string,
POSDiscAmt string,
ManufacturerCouponAmt string,
RegisterNumber string,
Account_Number_Masked string,
postransactionnumber string,
checkid string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_gold/transactions'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_tenders_history_gold
(
	sourcecustomernumber string,
	registernumber string,
	transactiontimestamp string,
	currency string,
	customer_name string,
	check_id string,
	sourcetransactionnumber string,
	sourceorganizationnumber integer,
	transactiondate date,
	tendercode string,
	tendername string,
	tenderamt double,
	accountnumbermasked string,
	cardusagetype string
 )ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
)LOCATION
  's3://bridg-client-ftp/dollargeneral/transformed/history_gold/tenders'
TBLPROPERTIES (
  'has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_transaction_item_history_gold (
 sourcetransactionitemnumber string,
 sourcetransactionnumber string,
 sourceorganizationnumber INTEGER,
 transactionitemtype string,
 subtype string,
 invoicedate date,
 shipdate date,
 sourceproductnumber string,
 quantity string,
 weight string,
 volume string,
 listprice string,
 currency string,
 salesrevenue string,
 discount string,
 costbasis string,
 tax string,
 shippingrevenue string,
 shippingcost string,
 shippingdiscount string,
 otherrevenue string,
 othercosts string,
 datecreated date,
 dt string,
 checkid string,
 transactiontimestamp string
 )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  'skip.header.line.count'='1'
)
LOCATION
  's3://bridg-client-ftp/dollargeneral/transformed/history_gold/line_item'
TBLPROPERTIES (
  'has_encrypted_data'='true',
  'transient_lastDdlTime'='1599234059')



CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_discounts_history_gold(
    datecreated string,
	sourcetransactionnumber string,
	sourcetransactionitemnumber string,
	sourceorganizationnumber string,
	invoicedate string,
	shipdate string,
	sourceproductnumber string,
	discountcode string,
	discountamt string,
	discounttype string,
	discountdescription string,
	check_id string,
	dt string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_gold/discounts'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_organization_history_gold (
SourceOrganizationNumber string,
OrgName string,
Status string,
OrgType string,
Subtype string,
ParentSourceOrganizationNumber string,
Country string,
OrgState string,
City string,
Address1 string,
Address2 string,
Zip string,
ExcludeAsClosestStore string,
DateCreated string,
CustomAttributes string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_gold/organization/'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_history_gold (
SourceProductNumber string,
ProductName string,
Description string,
ProductURL string,
ImageURL string,
ParentProductNumber string,
BrandName string,
MSRP string,
ListPrice string,
SalePrice string,
SaleCondition string,
Availability string,
AvailableQty string,
RecoStatus string,
ProductSize string,
Color string,
SourceProductCategoryNumber string,
DateCreated string,
CustomAttributes string,
Sourceproductcategoryname string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_gold/product/'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_category_history_gold (
SourceCategoryNumber string,
ProductCategoryName string,
SourceParentCategoryNumber string,
DateCreated string,
CustomAttributes string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_gold/product_category/'
TBLPROPERTIES ('has_encrypted_data'='true');


-------------------ARCHIVE-----------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_transaction_history_archive (
SourceTransactionNumber string,
SourceOrganizationNumber string,
Total string,
Currency string,
Discount string,
Tax string,
Type string,
TransactionTimeStamp string,
SourceCustomerNumber string,
DateCreated string,
Custom_Attributes string,
PromoAmt string,
CouponAmt string,
POSDiscAmt string,
ManufacturerCouponAmt string,
RegisterNumber string,
Account_Number_Masked string,
postransactionnumber string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_archive/transactions'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_tenders_history_archive
(
	sourcetransactionnumber BIGINT,
	sourceorganizationnumber INTEGER,
	transactiondate date,
	tendercode string,
	tendername string,
	tenderamt float,
	accountnumbermasked string,
	cardusagetype string
 )ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
)LOCATION
  's3://bridg-client-ftp/dollargeneral/transformed/history_archive/tenders'
TBLPROPERTIES (
  'has_encrypted_data'='true');

CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_transaction_item_history_archive(
 sourcetransactionitemnumber string,
 sourcetransactionnumber string,
 sourceorganizationnumber INTEGER,
 transactionitemtype string,
 subtype string,
 invoicedate date,
 shipdate date,
 sourceproductnumber string,
 quantity string,
 weight string,
 volume string,
 listprice string,
 currency string,
 salesrevenue string,
 discount string,
 costbasis string,
 tax string,
 shippingrevenue string,
 shippingcost string,
 shippingdiscount string,
 otherrevenue string,
 othercosts string,
 datecreated date
 )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  'skip.header.line.count'='1'
)
LOCATION
  's3://bridg-client-ftp/dollargeneral/transformed/history_archive/transaction_item'
TBLPROPERTIES (
  'has_encrypted_data'='true',
  'transient_lastDdlTime'='1599234059')



CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_trans_disc_xref_history_archive (
SourceTransactionItemNumber string,
DiscountCode string,
DiscountAmt double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_archive/trans_disc_xref'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_category_history_archive(
  sourcecategorynumber string,
  productcategoryname string,
  sourceparentcategorynumber string,
  datecreated string,
  customattributes string
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
)LOCATION
  's3://bridg-client-ftp/dollargeneral/transformed/history_archive/product_category'
TBLPROPERTIES (
  'has_encrypted_data'='true',
  'transient_lastDdlTime'='1597982252')


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_organization_history_archive (
SourceOrganizationNumber string,
OrgName string,
Status string,
OrgType string,
Subtype string,
ParentSourceOrganizationNumber string,
Country string,
OrgState string,
City string,
Address1 string,
Address2 string,
Zip string,
ExcludeAsClosestStore string,
DateCreated string,
CustomAttributes string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_archive/organization/'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_history_archive (
SourceProductNumber string,
ProductName string,
Description string,
ProductURL string,
ImageURL string,
ParentProductNumber string,
BrandName string,
MSRP string,
ListPrice string,
SalePrice string,
SaleCondition string,
Availability string,
AvailableQty string,
RecoStatus string,
ProductSize string,
Color string,
SourceProductCategoryNumber string,
DateCreated string,
CustomAttributes string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_archive/product'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_category_history_archive (
SourceCategoryNumber string,
ProductCategoryName string,
SourceParentCategoryNumber string,
DateCreated string,
CustomAttributes string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_archive/product_category/'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_discounts_history_archive(
  discountcode string,
  discountdescription string,
  discounttype string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/history_archive/discounts'
TBLPROPERTIES ('has_encrypted_data'='true');

