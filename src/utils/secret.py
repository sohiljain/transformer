# Copyright (c) 2020 Bridg Inc. All rights reserved.

import json
import boto3
from botocore.exceptions import ClientError

# from utils.utils import send_sns_alert

error_subject = "Transformer: Secret Retrieval Failure"

def send_sns_alert(subject, error_message):
    pass

class Secret:
    """
    This class provides methods to read from AWS Secrets Manager
    It returns objects based on the secret_name provided to the constructor
    @author Sohil Jain <sohil.jain@bridg.com>
    """

    def __init__(self, secret_name: str, secret_json: dict = None, region_name: str = "us-west-2"):
        """
        Constructor
        :param secret_name: name of the aws secret
        :param region_name: aws region
        """
        self.secret_name = secret_name
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name,
        )
        self.secret_json = secret_json or self.set_secret_json(client)

    def get_username(self):
        return self.get_secret('username')

    def get_password(self):
        return self.get_secret('password')

    def get_engine(self):
        return self.get_secret('engine')

    def get_host(self):
        return self.get_secret('host')

    def get_port(self):
        return self.get_secret('port')

    def get_dbclusteridentifier(self):
        return self.get_secret('dbClusterIdentifier')

    def get_account(self):
        return self.get_secret('account')

    def get_database(self):
        return self.get_secret('database')

    def get_schema(self):
        return self.get_secret('schema')

    def get_warehouse(self):
        return self.get_secret('warehouse')

    def get_role(self):
        return self.get_secret('role')

    def get_passphrase(self):
        return self.get_secret('passphrase')

    def set_secret_json(self, client):
        """
        returns aws secrets value in a dict format
        """
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=self.secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                error_msg = "The requested secret " + self.secret_name + " was not found"
                print(error_msg)
                send_sns_alert(error_subject, error_msg)
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                error_msg = f"The request was invalid due to : {e}"
                print(error_msg)
                send_sns_alert(error_subject, error_msg)
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                error_msg = f"The request had invalid params: {e}"
                print(error_msg)
                send_sns_alert(error_subject, error_msg)
        else:
            # Secrets Manager decrypts the secret value using the associated KMS CMK
            # Depending on whether the secret was a string or binary, only one of these fields will be populated
            if 'SecretString' in get_secret_value_response:
                text_secret_data = get_secret_value_response['SecretString']
                json_secret_data = json.loads(text_secret_data)
                return json_secret_data
            else:
                error_msg = f"Key 'SecretString' not found in {self.secret_name}"
                send_sns_alert(error_subject, error_msg)
                return ValueError(error_msg)

    def get_secret(self, secret: str):
        """
        returns aws secrets text value
        """

        if secret in self.secret_json:
            return self.secret_json[secret]
        else:
            error_msg = f"Key: '{secret}' not found in {self.secret_name}"
            send_sns_alert(error_subject, error_msg)
            return ValueError(error_msg)
