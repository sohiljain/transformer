pipeline {
  parameters {
    booleanParam(name: "DEPLOY_DG_TRANSFORMER_TO_TOWERBRIDG",
                 description: "Deploys DG TRANSFORMER to Towerbridg",
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

   	stage('Deploy CDP to DG TRANSFORMER Towerbridg') {
      when {
        allOf {
           expression { params.DEPLOY_DG_TRANSFORMER_TO_TOWERBRIDG }
        }
      }
      environment {
        	AWS_ACCESS_KEY_ID = credentials('aws-bridg2-id')
        	AWS_SECRET_ACCESS_KEY = credentials('aws-bridg2-secret')
        	DEPLOYMENT = 'development'
        	AWS_DEFAULT_REGION = 'us-west-2'
        	S3_BUCKET_RAW_DATA_1 = 'bridg-ftp-client'
        	BRIDG_ENV_NAME = 'dev-cdp'
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
 	      sh '$(aws ecr get-login --no-include-email --region us-west-2)'
//           sh 'aws s3 cp s3://bridg-devops-production/bin/deploy_bridg_service.py .'
          sh 'pip install --target ./src pyyaml'
          zip zipFile: 'build/dg_transformer_prepare.zip', archive: false, dir: 'src'
          sh 'serverless deploy --stage development --verbose'

      }
    }

    stage('Deploy CDP to DG TRANSFORMER Production') {
      when {
        allOf {
          branch "master"
          expression { params.DEPLOY_DG_TRANSFORMER_TO_PRODUCTION }
        }
      }
      environment {
        	AWS_ACCESS_KEY_ID = credentials('aws-bridg2-id')
        	AWS_SECRET_ACCESS_KEY = credentials('aws-bridg2-secret')
        	DEPLOYMENT = 'production'
        	AWS_DEFAULT_REGION = 'us-west-2'
        	S3_BUCKET_RAW_DATA_1 = 'bridg-client-ftp'
        	SNS_TOPIC_ARN_1 = 'dg-transformer-lambda-prod-sns'
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
 	      sh '$(aws ecr get-login --no-include-email --region us-west-2)'
          sh 'aws s3 cp s3://bridg-devops-production/bin/deploy_bridg_service.py .'
          sh 'aws s3 cp src s3://bridg-binary-registry/bridg-dollargeneral-transformer/ --recursive'
//           sh 'python3 deploy_bridg_service.py'
          sh 'pip install --target ./src pyyaml'
          zip zipFile: 'build/dg_transformer_prepare.zip', archive: false, dir: 'src'
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
