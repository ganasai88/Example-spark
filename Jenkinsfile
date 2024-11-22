pipeline {
    agent any

    environment {
        REPO_URL = 'https://github.com/ganasai88/Example-spark.git' // GitHub repository URL4
        VENV_DIR = 'venv'
        S3_DIR = 'S3-Terraform'
        EMR_DIR = 'EMR'
        ACCESS_KEY = credentials('AWS_ACCESS_KEY_ID')
        SECRET_KEY = credentials('AWS_SECRET_KEY_ID')
        REGION = 'us-east-2'
        S3_BUCKET = 'cloud-hosting01'
        STEP_NAME = 'Run Monthly Spark Job'
    }

    stages {
        stage('Checkout') {
            steps {

                git branch: 'main', url: "${REPO_URL}"

            }
        }
        stage('Upload Files to S3') {
            steps {
                 script {
                     sh '''
                     echo "Uploading files to S3 bucket ${S3_BUCKET}..."
                     # Sync the code to S3
                     aws s3 sync . s3://${S3_BUCKET} --exclude "*" --include "*.py" --include "*.json" --include "*.csv"
                     echo "Files uploaded successfully!"
                     '''
                 }
            }
        }
        stage('Find Running EMR Cluster') {
            steps {
                script {
                    // Get the cluster ID of the first running EMR cluster
                    def clusterId = sh(
                        script: '''
                        aws emr list-clusters \
                            --active \
                            --query "Clusters[?Status.State=='WAITING']|[0].Id" \
                            --region $REGION \
                            --output text
                        ''',
                        returnStdout: true
                    ).trim()
                    if (!clusterId) {
                        error "No running EMR cluster found!"
                    }
                    echo "Found EMR Cluster ID: ${clusterId}"
                    env.CLUSTER_ID = clusterId
                }
            }
        }
        stage('Add Step to EMR Cluster') {
            steps {
                script {
                   // Adding step to the running EMR cluster
                   def addStepCommand = """
                       aws emr add-steps \
                           --cluster-id ${env.CLUSTER_ID} \
                           --steps '[{
                               "Type": "Spark",
                               "Name": "${STEP_NAME}",
                               "ActionOnFailure": "CONTINUE",
                               "Args": [
                                   "--deploy-mode", "cluster", "s3://${S3_BUCKET}/main2.py",
                                   "--config", "s3://${S3_BUCKET}/s3config.json"
                               ]
                           }]' \
                           --region ${REGION}
                   """

                   sh addStepCommand
                   echo "Step added to EMR Cluster ID: ${env.CLUSTER_ID}"

                   //sleep(time: 60, unit: 'SECONDS')

                   // Poll for step completion status
                   def stepId = sh(script: """
                                    aws emr list-steps --cluster-id ${env.CLUSTER_ID} --region ${env.REGION} \
                                    --query "Steps[?Status.State=='RUNNING' || Status.State=='PENDING'] | sort_by(@, &Timeline.CreationDateTime) | [-1].Id" --output text
                                """, returnStdout: true).trim()

                   if (stepId == '') {
                       error "Failed to retrieve step ID!"
                   }

                   echo "Polling for step completion..."

                   // Polling for step completion
                   def stepStatus = ''
                   while (stepStatus != 'COMPLETED' && stepStatus != 'FAILED' && stepStatus != 'CANCELED') {
                       // Get the current step status
                       stepStatus = sh(script: "aws emr describe-step --cluster-id ${env.CLUSTER_ID} --step-id ${stepId} --region ${REGION} --query 'Step.Status.State' --output text", returnStdout: true).trim()
                       echo "Current step status: ${stepStatus}"

                       if (stepStatus == 'PENDING' || stepStatus == 'RUNNING') {
                           echo "Step is still running, waiting for completion..."
                           // Wait for a few seconds before polling again
                           sleep(time: 30, unit: 'SECONDS')
                       } else {
                           echo "Step finished with status: ${stepStatus}"
                       }
                   }

                   // Handle failure cases
                   if (stepStatus == 'FAILED' || stepStatus == 'CANCELED') {
                       error "EMR step failed or was canceled!"
                   }
                }
            }
        }
    }
}
