# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

'''
This is the main run script that decides what ETL to perform based on the parameters passed
'''

import argparse
import logging
from datetime import datetime
import yaml
import boto3

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s')

if __name__ == "__main__":
    logging.info("Starting Dollargeneral Transformer pipeline")

    # Add the arguments
    my_parser = argparse.ArgumentParser(description='Starting load data pipeline data to snowflake')
    my_parser.add_argument('--module', metavar='', type=str, help='Config yaml file', required=True)
    my_parser.add_argument('--date', metavar='', type=str, help='Date', required=False, default=datetime.now().strftime('%Y%m%d'))
    my_parser.add_argument('--root-dir', type=str, help='Root Directory of project', required=False, default='/code')
    my_parser.add_argument('--env', nargs='?', default='',
                           help='which branch in config-repo will contain the config files')
    my_parser.add_argument('--type', nargs='?', default='',
                           help='which sub-branch in config-repo will contain the config files')
    my_parser.add_argument('--schedule', nargs='?', default='',
                           help='schedule of file in config-repo daily or weekly')
    args = my_parser.parse_args()
    args_dt = args.date
    root_dir = args.root_dir

    # Config file based on env
    config_file = f'{args.env}-{args.type}-{args.schedule}-transformer.yml'
    config_file_path = f'{root_dir}/config/{args.env}/{config_file}'
    logging.info(f'Loading config from {config_file_path}')
    logging.info(f'Root directory has been set as {root_dir}')

    # Setup pyspark environment and config file if module is emr_process_gold
    if args.module == "emr_process_gold":
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        s3 = boto3.client('s3', region_name='us-west-2')
        s3_bucket = root_dir.replace('s3://', '').split('/')[0]
        s3_key = '/'.join(root_dir.replace('s3://', '').split('/')[1:])
        spark.sparkContext.addPyFile(f'{root_dir}/dg_transformer_prepare.zip')
        s3.download_file(s3_bucket, f'{s3_key}/config/{args.env}/{config_file}', config_file)
        config_file_path = config_file

    # Parse the yaml config file
    with open(config_file_path, 'r') as yml_file:
        yaml_cfg = yaml.safe_load(yml_file)
        yaml_cfg['root_dir'] = root_dir
        yaml_cfg['args_dt'] = args.date
        yaml_cfg['gnupghome'] = f'{root_dir}/gpghome'
        yaml_cfg['local_Path'] = f"{root_dir}/{yaml_cfg['local_Path']}"

    from utils.config import DgConfig
    dg_config: DgConfig = DgConfig(yaml_cfg)
    logging.info("configuration file passed for = {}".format(config_file_path))

    # Start batch_pgp_decrypt or emr_process_gold based on module passed in lambda
    if args.module == "batch_pgp_decrypt":
        from main.pgp_decrypt_upload import pgp_decrypt
        pgp_decrypt(dg_config, args_dt, root_dir, args.env, args.type, args.schedule)
    elif args.module == "emr_process_gold":
        from main.emr_process_gold import process_gold
        process_gold(spark, dg_config, args.type, args.schedule)
    else:
        raise ValueError(f'Invalid or no module value passed: {args.module}')
