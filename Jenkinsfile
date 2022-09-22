// # Copyright (c) 2020 Bridg Inc. All rights reserved.
// # @author Sohil Jain <sohil.jain@bridg.com>

pipeline {
  parameters {
    booleanParam(name: "DEPLOY_DG_TRANSFORMER_TO_TOWERBRIDG",
                 description: "Deploys DG TRANSFORMER to TowerBridg",
                 defaultValue: false)
    booleanParam(name: "DEPLOY_DG_TRANSFORMER_TO_PRODUCTION",
                 description: "Deploys DG TRANSFORMER to Production",
                 defaultValue: false)
    }

  agent any
  options {
    timestamps()
    skipDefaultCheckout()
  }

  stages {
    stage('Checkout') {
      steps {
        cleanWs()
        checkout scm
      }
    }

    stage('Deploy CDP to DG TRANSFORMER TowerBridg') {
      when {
        allOf {
          expression { params.DEPLOY_DG_TRANSFORMER_TO_TOWERBRIDG }
        }
      }
      environment {
        	DEPLOYMENT = 'development'
            AWS_ACCESS_KEY_ID = credentials('shared-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('shared-aws-secret-access-key')
        	AWS_DEFAULT_REGION = 'us-west-2'
        	S3_BUCKET_RAW_DATA_1 = 'dev-cdp-data-lake'
        	SNS_TOPIC_ARN_1 = 'dg-transformer-lambda-sns'
        	SNS_TOPIC_ARN_2 = 'data_ingestion_ftp_sync'
        	BRIDG_ENV_NAME = 'dev-cdp'
        	BATCH_JOBNAME = 'dev-cdp-dg-transformer'
            BATCH_JOBQUEUE = 'dev-cdp-que'
            BATCH_JOBDEFINITION = 'dev-cdp-dg-transformer'
            ALERT_SNS_PARAM = '/dev-cdp/SNS/Alarm'
        	BRIDG_1_ACCOUNT = credentials('bridg1-account-id')
        	BRIDG_CONFIG_URL = 'http://config.dev-cdp.towerbridg.com/'
            SUBNET_A = 'subnet-051cad2b010989d3a'
            SUBNET_B = 'subnet-06d4b6a3c6e279276'
            SUBNET_C = 'subnet-08f6d84463db5e1f8'
            SECURITY_GROUP = 'sg-0710b844e08d5cb2e'
      }
      steps {
          sh 'pip install --target ./src pyyaml'
          sh 'pip install --target ./src python-gnupg==0.4.6'
          sh 'pip install --target ./src pandas'
          zip zipFile: 'src/dg_transformer_prepare.zip', archive: false, dir: 'src'
          sh 'aws s3 rm s3://towerbridg-binary-registry/bridg-dollargeneral-transformer/ --recursive'
          sh 'aws s3 cp src s3://towerbridg-binary-registry/bridg-dollargeneral-transformer/ --recursive'
          sh 'aws s3 cp s3://bridg-devops-development/bin/deploy_bridg_service.py .'
          sh './deploy/login.sh'
          sh 'python3 deploy_bridg_service.py'
          sh 'serverless deploy --stage development --verbose'
      }
    }

    stage('Deploy CDP to DG TRANSFORMER Production') {
      when {
        allOf {
          expression { params.DEPLOY_DG_TRANSFORMER_TO_PRODUCTION }
        }
      }
      environment {
        	AWS_ACCESS_KEY_ID = credentials('aws-bridg2-id')
        	AWS_SECRET_ACCESS_KEY = credentials('aws-bridg2-secret')
        	DEPLOYMENT = 'production'
        	AWS_DEFAULT_REGION = 'us-west-2'
        	S3_BUCKET_RAW_DATA_1 = 'bridg-client-ftp'
        	SNS_TOPIC_ARN_1 = 'dg-transformer-lambda-sns'
        	SNS_TOPIC_ARN_2 = 'data_ingestion_ftp_sync'
        	BRIDG_ENV_NAME = 'cdp'
        	BRIDG_CONFIG_URL = 'http://config-cdp.towerbridg.com/'
        	BATCH_JOBNAME = 'cdp-dg-transformer'
            BATCH_JOBQUEUE = 'cdp-que'
            BATCH_JOBDEFINITION = 'cdp-dg-transformer'
            ALERT_SNS_PARAM = '/cdp/SNS/Alarm'
        	BRIDG_1_ACCOUNT = credentials('bridg1-account-id')
            SUBNET_A = 'subnet-080ca4bae7f28e8a9'
            SUBNET_B = 'subnet-0fa602d9b7d5ac155'
            SUBNET_C = 'subnet-072541dffe416a273'
            SECURITY_GROUP = 'sg-08bcfd652a860cf54'
      }
      steps {
          sh './deploy/login.sh'
 	      sh 'aws s3 cp s3://bridg-devops-production/bin/deploy_bridg_service.py .'
          sh 'python3 deploy_bridg_service.py'
          sh 'pip install --target ./src pyyaml'
          sh 'pip install --target ./src s3fs'
          sh 'pip install --target ./src boto3'
          sh 'pip install --target ./src python-gnupg==0.4.6'
          zip zipFile: 'src/dg_transformer_prepare.zip', archive: false, dir: 'src'
          sh 'aws s3 rm s3://bridg-binary-registry/bridg-dollargeneral-transformer/ --recursive'
          sh 'aws s3 cp src s3://bridg-binary-registry/bridg-dollargeneral-transformer/ --recursive'
          sh 'serverless deploy --stage production --verbose'
      }
    }

  }

  post {
      always {
          cleanWs()
      }
  }
}
