pipeline {
  parameters {
    booleanParam(name: "DEPLOY_DG_TRANSFORMER_TO_TOWERBRIDG",
                 description: "Deploys DG TRANSFORMER to Towerbridg",
                 defaultValue: false)
    booleanParam(name: "DEPLOY_DG_TRANSFORMER_TO_PRODUCTION",
                 description: "Deploys DG TRANSFORMER to Production",
                 defaultValue: false)
    booleanParam(name: "DEPLOY_DG_TRANSFORMER_TO_TESTING",
                 description: "Deploys DG TRANSFORMER to Testing",
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
      }
      steps {
 	      sh '$(aws ecr get-login --no-include-email --region us-west-2)'
          sh 'aws s3 cp s3://bridg-devops-development/bin/deploy_bridg_service.py .'
          sh 'aws s3 cp src/main/emr.py s3://bridg-devops-production/bin/deploy_bridg_service.py'
          sh 'python3 deploy_bridg_service.py'
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
      }
      steps {
 	      sh '$(aws ecr get-login --no-include-email --region us-west-2)'
          sh 'aws s3 cp s3://bridg-devops-production/bin/deploy_bridg_service.py .'
          sh 'aws s3 cp src/main/emr.py s3://bridg-devops-production/bin/deploy_bridg_service.py'
          sh 'python3 deploy_bridg_service.py'
      }
    }

    stage('Deploy CDP to DG TRANSFORMER Test') {
      when {
        allOf {
          branch "onb-394"
          expression { params.DEPLOY_DG_TRANSFORMER_TO_TESTING }
        }
      }
      environment {
        	AWS_ACCESS_KEY_ID = credentials('aws-bridg2-id')
        	AWS_SECRET_ACCESS_KEY = credentials('aws-bridg2-secret')
        	DEPLOYMENT = 'production'
        	AWS_DEFAULT_REGION = 'us-west-2'
        	S3_BUCKET_RAW_DATA_1 = 'bridg-ftp-client'
        	SNS_TOPIC_ARN_1 = 'dollargeneral-transformer-lambda-sns'
        	SNS_TOPIC_ARN_2 = 'dollargeneral-transformer-emr-sns'
      }
      steps {
 	      sh '$(aws ecr get-login --no-include-email --region us-west-2)'
          sh 'aws s3 cp src/main/emr_process_gold_daily.py s3://bridg-binary-registry/bridg-dollargeneral-transformer/'
          sh 'aws s3 cp src/main/emr_process_gold_historical.py s3://bridg-binary-registry/bridg-dollargeneral-transformer/'
          sh 'aws s3 cp s3://bridg-devops-production/bin/deploy_bridg_service.py .'
          sh 'python3 deploy_bridg_service.py'
      }
    }

  }

  post {
      always {
          cleanWs()
      }
  }
}
