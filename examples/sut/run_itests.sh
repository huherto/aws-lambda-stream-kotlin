#!/usr/bin/env bash
# runitest.sh Run integration tests and retrieve logs from localstack.


set -e    # (errexit): Exit immediately if a command exits with a non-zero status.
# set -x  # (xtrace): Print each command to stdout before executing it (useful for debugging).

# SCRIPT_DIR is the directory where the script is located.
# All the operations are relative to this location regardless of where the
# script is executed.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GW=$(realpath $SCRIPT_DIR/../../gradlew)

cd $SCRIPT_DIR
$GW integrationTest --rerun-tasks || true

LOGS=./.awslocal_logs
mkdir -p $LOGS
sleep 5 # Wait for the output to be ready to be retrieved.

# I use lnav to view the logs. This prefilter makes them easier to parse.
prefilter() { 
	jq -c '.events[]'
}


awslocal logs filter-log-events --log-group-name "/aws/lambda/sut-control-service-local-listener" | prefilter > $LOGS/control-service-listener.json
awslocal logs filter-log-events --log-group-name "/aws/lambda/sut-control-service-local-trigger"  | prefilter > $LOGS/control-service-trigger.json
awslocal logs filter-log-events --log-group-name "/aws/events/sut-event-hub-local-events"  | prefilter > $LOGS/event-hub-local-events.json
awslocal logs filter-log-events --log-group-name "/aws/events/sut-event-hub-local-faults"  | prefilter > $LOGS/event-hub-local-faults.json
awslocal logs filter-log-events --log-group-name "/aws/lambda/sut-shipment-bff-local-listener" | prefilter > $LOGS/shipment-bff-listener.json
awslocal logs filter-log-events --log-group-name "/aws/lambda/sut-shipment-bff-local-restapi" | prefilter > $LOGS/shipment-bff-restapi.json
awslocal logs filter-log-events --log-group-name "/aws/lambda/sut-shipment-bff-local-trigger" | prefilter > $LOGS/shipment-bff-trigger.json
