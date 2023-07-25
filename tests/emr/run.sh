#!/usr/bin/env bash

set -e

EMR_RUNTIME_ROLE="arn:aws:iam::874361956431:role/flowman-emr-role"
EMR_APPLICATION_PREFIX="s3://flowman-test/integration-tests/emr"
EMR_APPLICATION_JAR="$EMR_APPLICATION_PREFIX/emr-test-1.0-SNAPSHOT-emr.jar"

# Get current Flowman version
FLOWMAN_VERSION=$(mvn -f ../.. -q -N help:evaluate -Dexpression=project.version -DforceStdout)

wait_emr_application() {
    while :
    do
        EMR_APPLICATION_STATE=$(aws emr-serverless get-application \
            --application-id "$EMR_APPLICATION_ID" \
            | jq -r .application.state)
        echo "Waiting for EMR application $EMR_APPLICATION_ID, state is $EMR_APPLICATION_STATE"
        if [[ "$EMR_APPLICATION_STATE" == "$1" ]]; then
            echo "EMR application Successfully reached state $1"
            break
        fi
        sleep 3
    done
}

wait_emr_job() {
    while :
    do
        EMR_JOB_STATE=$(aws emr-serverless get-job-run \
            --application-id "$EMR_APPLICATION_ID" \
            --job-run-id "$EMR_JOB_RUN_ID" \
            | jq -r .jobRun.state)
        echo "Waiting for EMR job run $EMR_JOB_RUN_ID, state is $EMR_JOB_STATE"
        if [[ "$EMR_JOB_STATE" == "$1" ]]; then
            echo "EMR job run Successfully reached state $1"
            break
        fi
        sleep 3
    done
}


# Build package
mvn clean install -Dflowman.version=$FLOWMAN_VERSION
mvn flowman:deploy -DskipTests

# Execute tests via spark-submit
#spark-submit target/emr/emr-test-1.0-SNAPSHOT.jar -f flow test run


# Deploy an run in AWS
EMR_APPLICATION_ID=$(aws emr-serverless create-application \
    --release-label emr-6.10.0 \
    --type "SPARK" \
    --name "Flowman-$FLOWMAN_VERSION" \
    | jq -r .applicationId)
echo "Created EMR Serverless Application, applicationId=$EMR_APPLICATION_ID"
wait_emr_application "CREATED"


EMR_JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id "$EMR_APPLICATION_ID" \
    --execution-role-arn "$EMR_RUNTIME_ROLE" \
    --name job-run-name \
    --job-driver "{
        \"sparkSubmit\": {
          \"entryPoint\": \"$EMR_APPLICATION_JAR\",
          \"entryPointArguments\": [\"-B\", \"-f\", \"flow\", \"job\", \"build\", \"main\"],
          \"sparkSubmitParameters\": \"--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.instances=1\"
        }
    }" \
    | jq -r .jobRunId)
echo "Started EMR Serverless job, jobId=$EMR_JOB_RUN_ID"
wait_emr_job "SUCCESS"


aws emr-serverless stop-application \
    --application-id "$EMR_APPLICATION_ID"
wait_emr_application "STOPPED"


aws emr-serverless delete-application \
    --application-id "$EMR_APPLICATION_ID"
echo "Deleted EMR Serverless Application $EMR_APPLICATION_ID"
