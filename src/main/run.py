# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

'''
This script decides what ETL to perform based on the parameters passed
'''
import argparse
import logging, os
from datetime import datetime
# Uncomment this to run on local - os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom,org.apache.hadoop:hadoop-aws --conf spark.hadoop.fs.s3a.endpoint=s3.us-west-2.amazonaws.com'
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.addPyFile('s3://bridg-binary-registry/bridg-dollargeneral-transformer/dg_transformer_prepare.zip')

from main.emr_process_gold import process_gold
from main.pgp_decrypt_upload import pgp_decrypt
from utils.config import DgConfig
import yaml

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s')


if __name__ == "__main__":
    logging.info("Starting Dollargeneral Transformer pipeline")

    # Add the arguments
    my_parser = argparse.ArgumentParser(description='Starting load data pipeline data to snowflake')
    my_parser.add_argument('--module', metavar='', type=str, help='Config yaml file', required=True)
    my_parser.add_argument('--date', metavar='', type=str, help='Date', required=False, default=datetime.now().strftime('%Y%m%d'))
    my_parser.add_argument('--root-dir', type=str, help='Root Directory of project', required=False, default='/code')
    my_parser.add_argument('--branch', nargs='?', default='master',
                           help='which branch in config-repo will contain the config files')
    my_parser.add_argument('--env', nargs='?', default='',
                           help='which branch in config-repo will contain the config files')
    args = my_parser.parse_args()
    args_dt = args.date
    root_dir = args.root_dir
    logging.info(f'Root directory has been set as {root_dir}')
    logging.info(f'cwd - {os.getcwd()}')
    logging.info(f'listdir - {os.listdir()}')

    config_file = f'{args.env}-transformer.yml'
    config_file_path = f'{root_dir}/config/{config_file}' if args.module == "batch_pgp_decrypt" else config_file
    logging.info(f'Loading config from {config_file_path}')

    with open(config_file_path, 'r') as yml_file:
        yaml_cfg = yaml.safe_load(yml_file)
        yaml_cfg['root_dir'] = root_dir
        yaml_cfg['args_dt'] = args.date
        yaml_cfg['gnupghome'] = f'{root_dir}/gpghome'
        yaml_cfg['local_Path'] = f"{root_dir}/{yaml_cfg['local_Path']}"

    dg_config: DgConfig = DgConfig(yaml_cfg)
    logging.info("configuration file passed for = {}".format(config_file_path))

    if args.module == "batch_pgp_decrypt":
        try:
            pgp_decrypt(dg_config, args_dt, root_dir)

        except Exception as e:
            logging.error(f"Failed to publish SNS message {e} for BATCH job")
            # send_sns_alert(subject='PGP Decrypt Batch Failed', message=e)

    elif args.module == "emr_process_gold":
        process_gold(spark, dg_config, args_dt)
        # send_sns_alert(subject='EMR Process Gold Failed', message=e)
    else:
        raise ValueError(f'Invalid or no module value passed: {args.module}')
