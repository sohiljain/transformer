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
        	AWS_ACCESS_KEY_ID = credentials('shared-aws-secret-key-id')
        	AWS_SECRET_ACCESS_KEY = credentials('shared-aws-secret-access-key')
        	DEPLOYMENT = 'development'
        	AWS_DEFAULT_REGION = 'us-west-2'
        	S3_BUCKET_RAW_DATA_1 = 'bridg-ftp-client'
        	SNS_TOPIC_ARN_1 = 'dollargeneral-transformer-lambda-sns'
        	SNS_TOPIC_ARN_2 = 'dollargeneral-transformer-emr-sns'
        	BRIDG_ENV_NAME = 'dev-cdp'
        	BRIDG_CONFIG_URL = 'http://config.dev-cdp.towerbridg.com/'
        	BRIDG_1_ACCOUNT = credentials('bridg1-account-id')
        	SUBNET_A = 'subnet-051cad2b010989d3a'
            SUBNET_B = 'subnet-06d4b6a3c6e279276'
            SUBNET_C = 'subnet-08f6d84463db5e1f8'
            SECURITY_GROUP = 'sg-0710b844e08d5cb2e'
      }
      steps {
 	      sh '$(aws ecr get-login --no-include-email --region us-west-2)'
          sh 'aws s3 cp s3://bridg-devops-development/bin/deploy_bridg_service.py .'
          sh 'aws s3 cp src s3://towerbridg-binary-registry/bridg-dollargeneral-transformer/ --recursive'
          sh 'python3 deploy_bridg_service.py'
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
        	S3_BUCKET_RAW_DATA_1 = 'bridg-ftp-client'
        	SNS_TOPIC_ARN_1 = 'dg-transformer-lambda-sns'
        	SNS_TOPIC_ARN_2 = 'dollargeneral-transformer-emr-sns'
        	BRIDG_ENV_NAME = 'cdp'
        	BRIDG_CONFIG_URL = 'http://config.dev-cdp.towerbridg.com/'
        	BRIDG_1_ACCOUNT = credentials('bridg1-account-id')
        	SUBNET_A = 'subnet-087812d39e3c1abf7'
          SUBNET_B = 'subnet-0334a02247d7b732d'
          SUBNET_C = 'subnet-0428a47dfeff0c654'
          SECURITY_GROUP = 'sg-08bcfd652a860cf54'
      }
      steps {
 	      sh '$(aws ecr get-login --no-include-email --region us-west-2)'
          sh 'aws s3 cp s3://bridg-devops-production/bin/deploy_bridg_service.py .'
          sh 'aws s3 cp src s3://bridg-binary-registry/bridg-dollargeneral-transformer/ --recursive'
          //sh 'python3 deploy_bridg_service.py'
          //zip zipFile: 'build/dg_transformer_prepare.zip', archive: false, dir: 'src'
          //sh 'serverless deploy --stage production --verbose'
      }
    }

  }

  post {
      always {
          cleanWs()
      }
  }
}
