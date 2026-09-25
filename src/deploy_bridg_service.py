#!/usr/bin/env python

"""deploy_bridg_service.py: Deploys a Bridg ecs service from a bridg_service.yml config file. This script should be
executed in the same directory as the Dockerfile and bridg_service.yml"""
import sys

__author__ = 'Adam Vondersaar'
__copyright__ = 'Copyright 2018, Bridg'


import os

import yaml
import boto3
import docker
from botocore.exceptions import ClientError, WaiterError
from troposphere.ssm import Parameter
from troposphere import Template, Ref
from troposphere.ecr import Repository
from troposphere.elasticloadbalancingv2 import TargetGroup, Matcher, ListenerRule, Condition, Action
from troposphere.logs import LogGroup


ECS = boto3.client('ecs')
CFN = boto3.client('cloudformation')
ECR = boto3.client('ecr')
PS = boto3.client('ssm')
BATCH = boto3.client('batch')
SESSION = boto3.session.Session()


# TODO Create checks for config file
# TODO use more secret and parameter store gets
# TODO We should pull from a QA passed docker binary instead of building from the source and docker file. This will
# suffice for the initial launch of CDP

class BridgServiceDeploy:


    def __init__(self) -> None:
        # Production or development environmental variables. This should be set by jenkins
        self.deployment = str(os.environ.get('DEPLOYMENT')).lower()
        self.release_version = os.environ.get('RELEASE_VERSION')
        self.build_number = os.environ.get('BUILD_NUMBER')
        self.branch = str(os.environ.get('BRANCH_NAME')).lower()
        self.config = self.get_config()
        self.SERVICE = self.config['serviceName']
        self.deploy_config = self.config[self.deployment]
        self.batch = True if 'type' in self.deploy_config and str(self.deploy_config['type']).lower() == 'batch' else False
        self.ENVIRONMENT_NAME = self.deploy_config['environmentName']
        self.STACK_NAME = self.SERVICE_NAME = self.TARGET_GROUP_NAME = '{}-{}'.format(self.ENVIRONMENT_NAME, self.SERVICE)
        self.LOG_GROUP = '/{}/services/{}'.format(self.ENVIRONMENT_NAME, self.SERVICE)
        self.VPC_ID = PS.get_parameter(Name='/{}/Networking/VPC'.format(self.ENVIRONMENT_NAME))['Parameter']['Value']
        if 'clusterName' in self.deploy_config:
            self.CLUSTER = self.deploy_config['clusterName']
            if 'albId' in self.deploy_config:
                self.alb_id = self.deploy_config['albId']
            else:
                self.alb_id = 'Core' if 'core' in str(self.CLUSTER).lower() else 'Data'
            self.ALB_LISTENER = PS.get_parameter(
                Name='/{}/ALB/Listener/{}'.format(self.ENVIRONMENT_NAME, self.alb_id)
            )['Parameter']['Value']

    @staticmethod
    def get_config() -> dict:
        with open('bridg_service.yml') as config_file:
            return yaml.load(config_file)

    def get_listener_rules(self, template: Template, target_group: TargetGroup) -> None:
        listener_priority = int(self.deploy_config['albPriority'])
        for condition in self.deploy_config['listenerConditions']:
            template.add_resource(
                ListenerRule(
                    'ListenerRule{}'.format(listener_priority),
                    ListenerArn=self.ALB_LISTENER,
                    Priority=listener_priority,
                    Conditions=[
                        Condition(
                            Field=condition['Type'],
                            Values=[condition['Value']]
                        )
                    ],
                    Actions=[
                        Action(
                            TargetGroupArn=Ref(target_group),
                            Type='forward'
                        )
                    ]

                )
            )
            listener_priority += 1

    def parse_parameter(self, name: str) -> str:
        values = name.split('::')
        parameter = PS.get_parameter(Name=values[1])['Parameter']
        if parameter['Type'] == 'String':
            return parameter['Value']
        if parameter['Type'] == 'StringList':
            if 'RDS' in name:
                string_list = parameter['Value'].spit(',')
                return '{}:{}'.format(string_list[0], string_list[1])
            else:
                return parameter['Value']

    @staticmethod
    def get_parameter(path: str) -> str:
        return PS.get_parameter(Name=path)['Parameter']['Value']

    def get_parameter_or_arn(self, path: str) -> str:
        if 'arn:aws' in str(path).lower():
            return path
        else:
            return self.get_parameter(path)

    # Parse environmental variables from the bridg_service.yml config file
    def get_environment_vars(self) -> list:
        env_vars = []

        for env_entry in self.deploy_config['environmentalVariables']:
            if 'SSM' in str(env_entry['Value']):
                name = '/{}/Services/{}/{}'.format(self.ENVIRONMENT_NAME, self.SERVICE, env_entry['Name'])
                if 'SSM::' in str(env_entry['Value']):
                    env_vars.append(
                        {
                            'name': env_entry['Name'],
                            'value': self.parse_parameter(env_entry['Value'])
                        }
                    )
                else:
                    env_vars.append(
                        {
                            'name': env_entry['Name'],
                            'value': PS.get_parameter(Name=name)['Parameter']['Value']
                        }
                    )
            else:
                env_vars.append(
                    {
                        'name': env_entry['Name'],
                        'value': str(env_entry['Value'])
                    }
                )
        return env_vars

    # For secure parameters create a parameter store entry. Values are updated manually
    def get_parameters(self, template: Template, init: bool=False) -> None:
        x = 0
        for env_entry in self.deploy_config['environmentalVariables']:
            try:
                if env_entry['Value'] == 'SSM':
                    name = '/{}/Services/{}/{}'.format(self.ENVIRONMENT_NAME, self.SERVICE, env_entry['Name'])
                    template.add_resource(
                        Parameter(
                            'Parameter{}'.format(x),
                            Name=name,
                            Type='String',
                            Value='Null' if init else PS.get_parameter(Name=name)['Parameter']['Value']
                        )
                    )
                    x += 1
            except KeyError as e:
                raise Exception('Error in environmental variables format. Check for typos.')

    # Template to create just the repository to allow the container to be pushed
    def get_init_template(self) -> str:
        t = Template()
        t.add_resource(
            Repository(
                'Repository',
                RepositoryName='{}/{}'.format(self.ENVIRONMENT_NAME, self.SERVICE)
            )
        )
        self.get_parameters(
            template=t,
            init=True
        )
        return t.to_yaml()

    # Full template after container is pushed in to ECR
    def get_template(self) -> str:
        t = Template()
        t.add_resource(
            Repository(
                'Repository',
                RepositoryName='{}/{}'.format(self.ENVIRONMENT_NAME, self.SERVICE)
            )
        )
        self.get_parameters(
            template=t,
            init=False
        )

        t.add_resource(
            LogGroup(
                'Logging',
                LogGroupName=self.LOG_GROUP,
                RetentionInDays=30
            )
        )

        if not self.batch:
            # Generate ECS style definitions
            # If load balancer is required
            if 'listenerConditions' in self.deploy_config:
                target_group = t.add_resource(
                    TargetGroup(
                        'TargetGroup',
                        Name=self.TARGET_GROUP_NAME,
                        HealthCheckIntervalSeconds=20,
                        HealthCheckPath=self.deploy_config['healthCheckPath'],
                        HealthCheckProtocol='HTTP',
                        HealthCheckTimeoutSeconds=19,
                        HealthyThresholdCount=2,
                        Matcher=Matcher(
                            HttpCode='200-499'
                        ),
                        Port=self.deploy_config['containerPort'],
                        Protocol='HTTP',
                        UnhealthyThresholdCount=5,
                        VpcId=self.VPC_ID
                    )
                )

                self.get_listener_rules(
                    template=t,
                    target_group=target_group
                )

        result = t.to_yaml()

        self.print_offset_line(result)

        return result

    @staticmethod
    def print_offset_line(line) -> None:
        if isinstance(line, dict):
            line = yaml.dump(line)
        print('\n{}\n'.format(line))

    def get_container_definition(self) -> dict:
        return {
            'name': self.SERVICE_NAME,
            'essential': True,
            'image': self.get_image(),
            'memoryReservation': self.deploy_config['memory'],
            'cpu': self.deploy_config['cpu'],
            'portMappings': [
                {
                    'containerPort': self.deploy_config['containerPort']
                }
            ],
            'logConfiguration': {
                'logDriver': 'awslogs',
                'options': {
                    'awslogs-group': self.LOG_GROUP,
                    'awslogs-region': SESSION.region_name,
                    'awslogs-stream-prefix': self.SERVICE
                }
            },
            'environment': self.get_environment_vars()
        }

    def is_service_exist(self) -> bool:
        try:
            for service in ECS.describe_services(cluster=self.CLUSTER, services=[self.STACK_NAME])['services']:
                if service['serviceName'] == self.STACK_NAME and service['status'].lower() == 'active':
                    return True
            return False
        except ClientError:
            return False

    def is_stack(self) -> bool:
        try:
            CFN.describe_stacks(StackName=self.STACK_NAME)
            return True
        except ClientError:
            return False

    def get_stack_resources(self) -> dict:
        return CFN.describe_stack_resources(
            StackName=self.STACK_NAME
        )['StackResources']

    def get_repository(self) -> str:
        resources = self.get_stack_resources()
        for resource in resources:
            if resource['ResourceType'] == 'AWS::ECR::Repository':
                repo = resource['PhysicalResourceId']
                return ECR.describe_repositories(
                    repositoryNames=[repo]
                )['repositories'][0]['repositoryUri']

    def get_target_group(self) -> str:
        resources = self.get_stack_resources()
        for resource in resources:
            if resource['ResourceType'] == 'AWS::ElasticLoadBalancingV2::TargetGroup':
                return resource['PhysicalResourceId']

    def get_tag(self) -> str:
        if self.release_version:
            return self.release_version

        if self.branch == 'master' or self.branch == 'main':
            return self.build_number

        return '{}-{}'.format(self.branch.replace('/', '_'), self.build_number)

    def get_image(self, _tag: str=None) -> str:
        repo = self.get_repository()
        return '{}:{}'.format(repo, _tag if _tag else self.get_tag())

    def create_stack(self) -> None:
        print('Creating stack {}'.format(self.STACK_NAME))
        CFN.create_stack(
            StackName=self.STACK_NAME,
            TemplateBody=str(self.get_init_template())
        )
        print('Waiting for stack creation')
        try:
            waiter = CFN.get_waiter('stack_create_complete')
            waiter.wait(
                StackName=self.STACK_NAME,
                WaiterConfig={
                    'Delay': 60,
                    'MaxAttempts': 15
                }
            )
        except WaiterError as e:
            print('Waiter time out')
            print(str(e))

    def update_stack(self) -> None:
        self.print_offset_line('Updating stack')
        try:
            CFN.update_stack(
                StackName=self.STACK_NAME,
                TemplateBody=self.get_template(),
            )
            self.print_offset_line('Waiting for stack update')
            try:
                waiter = CFN.get_waiter('stack_update_complete')
                waiter.wait(
                    StackName=self.STACK_NAME,
                    WaiterConfig={
                        'Delay': 60,
                        'MaxAttempts': 15
                    }
                )
            except WaiterError as e:
                raise Exception('Waiter time out, reason {}'.format(str(e)))
        except ClientError as e:
            if 'No updates are to be performed' in str(e):
                self.print_offset_line('No cloudformation updates required')
                return

            raise Exception('Stack update failed, reason: {}'.format(str(e)))

        self.print_offset_line('Stack updated')

    def register_batch_job(self) -> None:
        self.print_offset_line('Registering batch job definition')
        job_def = {
            'type': 'container',
            'jobDefinitionName': self.SERVICE_NAME,
            'parameters': {},
            'containerProperties': {
                'image': self.get_image(),
                'environment': self.get_environment_vars(),
                'memory': self.deploy_config['memory'],
                'vcpus': self.deploy_config['cpu']
            }
        }

        if 'taskRole' in self.deploy_config:
            self.print_offset_line('Task Role Detected')
            job_def['containerProperties']['jobRoleArn'] = self.get_parameter_or_arn(self.deploy_config['taskRole'])

        self.print_offset_line(job_def)

        job_def_arn = BATCH.register_job_definition(**job_def)['jobDefinitionArn']

        self.print_offset_line(job_def_arn)

        PS.put_parameter(
            Name='/{}/Batch/Jobs/{}'.format(self.ENVIRONMENT_NAME, str(self.SERVICE).lower()),
            Type='String',
            Value=job_def_arn,
            Overwrite=True
        )

    def register_task_def(self) -> str:
        self.print_offset_line('Registering task definition')
        family = self.SERVICE_NAME

        task_def = {
            'family': family,
            'networkMode': 'bridge',
            'containerDefinitions': [
                self.get_container_definition()
            ]
        }

        if 'taskRole' in self.deploy_config:
            task_def['taskRoleArn'] = self.get_parameter_or_arn(self.deploy_config['taskRole'])

        self.print_offset_line(task_def)

        result = ECS.register_task_definition(**task_def)['taskDefinition']['taskDefinitionArn']

        self.print_offset_line(result)

        return result

    def update_service(self) -> None:
        task_def_arn = self.register_task_def()

        service_template = {
            'cluster': self.CLUSTER,
            'desiredCount': self.deploy_config['desiredCount'],
            'taskDefinition': task_def_arn
        }

        self.print_offset_line(service_template)

        if self.is_service_exist():
            self.print_offset_line('Updating service')

            service_template['service'] = self.SERVICE_NAME

            # TODO Add logic to handle load balancer update.
            #  ECS doesnt support load balancer update.
            #  If it is updated we have delete old and create a new service
            #  or use a task set
            self.print_offset_line(ECS.update_service(**service_template))
        else:
            self.print_offset_line('Creating service')

            service_template['launchType'] = 'EC2'
            service_template['serviceName'] = self.SERVICE_NAME

            if 'listenerConditions' in self.deploy_config:
                service_template['loadBalancers'] = [
                    {
                        'containerName': self.STACK_NAME,
                        'containerPort': self.deploy_config['containerPort'],
                        'targetGroupArn': self.get_target_group()
                    }
                ]
            self.print_offset_line(ECS.create_service(**service_template))

    def build_docker(self):
        self.print_offset_line('Building container')

        image = self.get_image()

        self.print_offset_line(image)

        client = docker.APIClient(base_url='unix://var/run/docker.sock')

        work_dir = os.getcwd()
        for line in client.build(path=work_dir, tag=image, decode=True):
            print(line)
            if 'error' in line:
                raise Exception('Error building docker image: {}'.format(line['error']))

        client.tag(image, self.get_image('latest'))

        self.print_offset_line('image created')

        for line in client.push(self.get_repository(), stream=True, decode=True):
            print(line)
            if 'error' in line:
                raise Exception('Error pushing docker image: {}'.format(line['error']))

        self.print_offset_line('image pushed')


if __name__ == '__main__':
    try:
        deploy = BridgServiceDeploy()
        if not deploy.is_stack():
            deploy.create_stack()
        deploy.update_stack()
        deploy.build_docker()
        if deploy.batch:
            deploy.register_batch_job()
        else:
            deploy.update_service()
    except Exception as e:
        print(e)
        exit(1)
