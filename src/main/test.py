import pyspark as pyspark
from pyspark import SparkContext

import boto3, os

s3 = boto3.client('s3', region_name='us-west-2')
session = boto3.session.Session(profile_name='bdl')

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell"
from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", session.get_credentials().access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", session.get_credentials().secret_key) \
        .getOrCreate()

spark.read.csv('s3a://dev-cdp-data-lake/small.csv', sep=',', header=True, nullValue='\\N').show()


# sts_connection = session.client('sts')
# response = sts_connection.assume_role(RoleArn='arn:aws:iam::020970917333:role/Dev', RoleSessionName='THIS_SESSIONS_NAME' ,DurationSeconds=3600)
# credentials = response['Credentials']
#
# spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key', session.get_credentials().token)
#
# spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.assumed.role.session.name', 'THIS_SESSIONS_NAME')
# # spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider')
# # spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.assumed.role.arn', 'arn:aws:iam::020970917333:role/dev-cdp-data-lake-dev')
#
# spark.read.csv('s3a://dev-cdp-data-lake/small.csv', sep=',', header=True, nullValue='\\N').show()
#
#
# spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key', credentials['SecretAccessKey'])
# spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.session.token', credentials['SessionToken'])
#
# #
# #
# # s3_resource=boto3.resource(
# #     's3',
# #     aws_access_key_id=credentials['AccessKeyId'],
# #     aws_secret_access_key=credentials['SecretAccessKey'],
# #     aws_session_token=credentials['SessionToken'],
# # )
# #
# # # Use the Amazon S3 resource object that is now configured with the
# # # credentials to access your S3 buckets.
# # for bucket in s3_resource.buckets.all():
# #     print(bucket.name)
# #
# # session.get_credentials()
# #
# # # s3_object = s3_resource.meta.client.get_object(Bucket="dev-cdp-data-lake", Key="dollargeneral/transformed/staging/aurus/ADTFF_5_4_4_20201226.csv")
# #
# spark.read.csv('s3a://dev-cdp-data-lake/dollargeneral/transformed/staging/aurus/ADTFF_5_4_4_20201226.csv', sep=',', header=True, nullValue='\\N').show()
