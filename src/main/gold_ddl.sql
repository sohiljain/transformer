CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_transaction_gold (
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
Account_Number_Masked string
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/gold/transactions'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_tenders_gold
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
  's3://bridg-client-ftp/dollargeneral/transformed/gold/tenders'
TBLPROPERTIES (
  'has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_transaction_item_gold (
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
  's3://bridg-client-ftp/dollargeneral/transformed/gold/line_item'
TBLPROPERTIES (
  'has_encrypted_data'='true',
  'transient_lastDdlTime'='1599234059')



CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_discounts_gold(
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
	check_id string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '|',
  'field.delim' = '|',
  'collection.delim' = 'undefined',
  'mapkey.delim' = 'undefined',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/gold/discounts'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_organization_gold (
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/gold/organization/'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_gold (
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/gold/product/'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_category_gold (
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/gold/product_category/'
TBLPROPERTIES ('has_encrypted_data'='true');

--STAGING

create external table transformer.dg_aurus
(
	store_id integer,
	transaction_id string,
	store_transaction_date_time string,
	host_transaction_date_time string,
	customer_name string,
	masked_card_number string,
	approval_code string,
	pos_transaction_number string,
	approved_amount double,
	pos_register_number integer
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  "skip.header.line.count"="1"
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/staging/aurus/'
TBLPROPERTIES ('has_encrypted_data'='false');

-------------------ARCHIVE-----------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_transaction_archive (
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/archive/transactions'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_tenders_archive
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
  's3://bridg-client-ftp/dollargeneral/transformed/archive/tenders'
TBLPROPERTIES (
  'has_encrypted_data'='true');

CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_transaction_item_archive(
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
  's3://bridg-client-ftp/dollargeneral/transformed/archive/transaction_item'
TBLPROPERTIES (
  'has_encrypted_data'='true',
  'transient_lastDdlTime'='1599234059')



CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_trans_disc_xref_archive (
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/archive/trans_disc_xref'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_category(
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
  's3://bridg-client-ftp/dollargeneral/transformed/archive/product_category'
TBLPROPERTIES (
  'has_encrypted_data'='true',
  'transient_lastDdlTime'='1597982252')


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_organization_archive (
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/archive/organization/'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_archive (
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/archive/product'
TBLPROPERTIES ('has_encrypted_data'='true');


CREATE EXTERNAL TABLE IF NOT EXISTS transformer.dg_product_category_archive (
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
) LOCATION 's3://bridg-client-ftp/dollargeneral/transformed/archive/product_category/'
TBLPROPERTIES ('has_encrypted_data'='true');

