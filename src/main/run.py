# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

'''
This script decides what ETL to perform based on the parameters passed
'''
import argparse
import logging
from datetime import datetime

import yaml
# from git import Repo

from main.emr_process_gold import process_gold
from main.pgp_decrypt_upload import pgp_decrypt
from utils.config import DgConfig
# from utils.utils import get_git_credentials

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s')


# def get_code(username, password, branch):
#     """Method to checkout config-repository"""
#     remote = f"https://{username}:{password}@github.com/Bridg/config-repository.git"
#     repo = Repo.clone_from(remote, 'config-repo')
#     repo.git.checkout(branch)
#     logging.info("Git checkout complete for branch {}".format(branch))


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

    # try:
    #     # Running on production and getting config file from git
    #     git_credentials = get_git_credentials('/prod/bdl/git')
    #     get_code(git_credentials.git_username, git_credentials.git_password, args.branch)
    #     environment = 'prod' if args.branch == 'master' else 'dev'
    #     logging.info("Environment selected {}".format(environment))
    #     config_file_path = 'config-repo' + '/' + 'bridg-dollargeneral-transformer' + '/' + environment \
    #                        + '/transformer.yml'  # if branch is master this should be prod else dev
    # except Exception as e:
    #     # Running on local and getting config file from local path
    config_file = f'{args.env}-transformer.yml'
    config_file_path = f'{root_dir}/config/{config_file}'

    with open(config_file_path, 'r') as yml_file:
        yaml_cfg = yaml.safe_load(yml_file)
        yaml_cfg['root_dir'] = root_dir
        yaml_cfg['args_dt'] = args.date
        yaml_cfg['gnupghome'] = f'{root_dir}/gpghome'
        yaml_cfg['local_Path'] = f"{root_dir}/{yaml_cfg['local_Path']}"

    dg_config: DgConfig = DgConfig(yaml_cfg)
    logging.info("configuration file passed for = {}".format(config_file_path))

    if args.module == "batch_pgp_decrypt":
        pgp_decrypt(dg_config, args_dt, root_dir)

    elif args.module == "emr_process_gold":
        process_gold(dg_config, args_dt)

    else:
        raise ValueError(f'Invalid or no module value passed: {args.module}')
