# bridg-rs-snowflake-etl

## Description

This git repository contains the code for all Bridg ETLs that load data into Snowflake. Changes to ETLs are deployed using jenkins and these ETLs
run as batch jobs on AWS bridg2 account. 

## How to run it?

There is no easy way to run this project locally currently but there is going to be an easier way to run it soon. Stay tuned!

If you want to run these ETLs in production, these ETLs are being run as batch jobs on AWS, so you can submit a job with job definition *cdp-data-lake-etl* (pick the latest job definition) and command line argument _--config {config_file_path}_.

#### Config parameter values for various ETLs

- ETL for Normalized ORC files -  _--config config/normalized_orc.yml_
- ETL for Redshift - _--config config/redshift.yml_
- ETL for Postgres (mobile data) - _--config config/postgres.yml_
- ETL for Statistics data - _--config config/statistics/mysql.yml_
- ETL for Membership data - _--config config/membership/mysql.yml_
- ETL for Data science core insights data - _--config config/core_insights.yml_

