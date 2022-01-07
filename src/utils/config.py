# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

import logging
from utils.secret import Secret

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format="%(message)s")


class DgConfig:

    def __init__(self, yaml_cfg):
        """
            parameterized constructor that returns configuration object.

        :rtype: EtlConfig instance
        """
        logging.info("Initializing DollarGeneral ETL Config")

        self.yaml_cfg = yaml_cfg
        self.table_list = yaml_cfg.get('table_list', '')
        self.bucket = yaml_cfg.get('bucket', '')
        self.s3a_bucket = yaml_cfg.get('s3a_bucket', '')
        self.s3_staging_path = yaml_cfg.get('s3_staging_path', '')
        self.s3_staging_path_1010 = yaml_cfg.get('s3_staging_path_1010', '')
        self.s3_tmp_path = yaml_cfg.get('s3_tmp_path', '')
        self.s3_archive_path = yaml_cfg.get('s3_archive_path', '')
        self.remote_1010_path = yaml_cfg.get('remote_1010_path', '')
        self.remote_Aurus_path = yaml_cfg.get('remote_Aurus_path', '')
        self.local_Path = yaml_cfg.get('local_Path', '')
        self.gnupghome = yaml_cfg.get('gnupghome', '')
        self.secret_name = yaml_cfg.get('secret', '')
        self.passphrase = Secret(self.secret_name).get_passphrase()
        self.table_list = yaml_cfg.get('table_list', '')
        self.s3_gold_path= yaml_cfg.get('s3_gold_path','')
        self.table_queries = yaml_cfg.get('table_queries', '')
        self.cols = yaml_cfg.get('cols', '')
        self.s3_staging_path_weekly_1010 = yaml_cfg.get('s3_staging_path_weekly_1010','')
        self.s3_archive_path_weekly = yaml_cfg.get('s3_archive_path_weekly','')
        self.aurus_start_date = yaml_cfg.get('aurus_startdate', '20200910')
