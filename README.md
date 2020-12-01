# bridg-dollargeneral-transformer

## Description

This git repository contains the code for all Bridg Dollar General Transformation that load data into Snowflake. Changes to ETLs are deployed using jenkins and these ETLs
run as batch jobs on AWS bridg2 account. 

Dollargeneral Transformer Architecture and path structure - https://bridgteam.atlassian.net/wiki/spaces/PBD/pages/1195048961/Dollargeneral+Transformer+Architecture+and+path+structure


# How job is triggered

##### Job is triggered by lambda in lambda_triggered_batch.py

Start job according to message value in s3 record event-
1. Trigger Batch job by sns notification of new s3 files  
2. Trigger EMR job by sns notification from batch
3. Trigger Batch job for manual back-fill by sending test event to lambda

Both Batch and EMR Job are triggered inside lambda and starter script is run.py

####Steps:
    1. Download raw Aurus/1010 files from the ftp bucket according to args_dt date/date_range provided
    2. PGP-Decrypt the file using python library
    3. Upload the decrypted file to s3 transformed staging directory for gold processing
    4. Upload the decrypted file to s3 transformed archive directory for backup
    5. Transform the decrypted file to a tmp directory
    6. Count checks and validations. If failed then abort
    7. Move tmp files to gold directory with proper names

#####Sample s3 record event - 
    {'Records': [{'EventSource': 'aws:sns', 'EventVersion': '1.0', 'EventSubscriptionArn': 'arn:aws:sns:us-west-2:992345807264:data_ingestion_ftp_sync:47d9beda-ac02-4725-92a6-36241b5b3c79', 'Sns': {'Type': 'Notification', 'MessageId': '49911010-c1ad-5d77-b1c9-482295212058', 'TopicArn': 'arn:aws:sns:us-west-2:992345807264:data_ingestion_ftp_sync', 'Subject': 'Amazon S3 Notification', 'Message': '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-west-2","eventTime":"2020-11-20T23:33:28.865Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AIDAYV5P5EDFEXI5RCHFI"},"requestParameters":{"sourceIPAddress":"99.246.58.228"},"responseElements":{"x-amz-request-id":"0399A0A826CA65D4","x-amz-id-2":"RKdS/p0a38CUCIdhEV+OkHdaQ3CcTZWUP7To9Ro1YKIN5Rj98xc4SmBClmtHBguuwoR5DlmCJQBBOvVqdjQ0c5uGzpIiS+ic"},"s3":{"s3SchemaVersion":"1.0","configurationId":"data-ingestion-ftp-sync-event","bucket":{"name":"bridg-client-ftp","ownerIdentity":{"principalId":"A2U5CZ1KQU2QJE"},"arn":"arn:aws:s3:::bridg-client-ftp"},"object":{"key":"dollargeneral/transformed/logmirror.ctrl","size":48,"eTag":"67320d87613c0c64b9f28ef914e86b69","sequencer":"005FB8524B2AA94D4D"}}}]}', 'Timestamp': '2020-11-20T23:33:32.026Z', 'SignatureVersion': '1', 'Signature': 'LzbpiTlWSllwiHwXuDgm9XXvGE5BA4JSBMNKywp+BmepHxYP49TOHxspB7zbf0I3LFuQHBBD0rLeMQSpTxOaqqIvFf6uZSuaaFdvWPKuSRdqmL0cL8M0lQmtIIFMJnRoNArTIDHlGjcj0xV3eTzkaxSYYmPMZylILfG/41qS8g8sSktysbmAyZJ9SFO6V4PasZXOOuy9+NDLKoQB9zetHjjpqS6rCQMdeLZlAhbEgvub2mMRbMLJZv2OBP7zxko3NEhx7BNoNv6Twuea2ydlWRy/36iOZuIB8sMiOYmcxqyaan3HIz71Irprek1wG2klTZj0ULz1pUnCkDN+Eboxng==', 'SigningCertUrl': 'https://sns.us-west-2.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem', 'UnsubscribeUrl': 'https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:992345807264:data_ingestion_ftp_sync:47d9beda-ac02-4725-92a6-36241b5b3c79', 'MessageAttributes': {}}}]}

# Backfill 
For Backfill: The date range needs to be provided along with the message as 'Manual' in record event.
Example for sample record event:

date_value can be of three types -
1. dt1:dt2 - Backfill all dates between dt1 and dt2 inclusive
2. dt1,dt2,dt3.. - Backfill all dates specified by comma delimiter
3. dt - Backfill for date dt

##### Example event -
    {"Records":[{"Sns":{"Message":"Manual","DateValue":"20200101:20200105"}}]}


#Run on dev
1. put env_detail as dev and pass DEV environment files as mentioned above
2. run lamdda - put env_detail as dev and pass DEV environment files
