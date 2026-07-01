#!/usr/bin/env bash


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GW=$(realpath $SCRIPT_DIR/../../gradlew)

cd $SCRIPT_DIR

LOGS=./.awslocal_logs
mkdir -p $LOGS

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
awslocal logs filter-log-events --log-group-name "/aws/kinesisfirehose/sut-event-fault-monitor-local-DeliveryStream" | prefilter > $LOGS/event-fault-monitor.json
awslocal logs filter-log-events --log-group-name "/aws/lambda/sut-event-fault-monitor-local-transform" | prefilter > $LOGS/event-fault-monitor-transform.json
awslocal logs filter-log-events --log-group-name "/aws/lambda/sut-regional-health-check-local-checkHealthApi" | prefilter > $LOGS/regional-health-check-checkHealthApi.json
