#!/usr/bin/env bash

set -x -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GW=$(realpath $SCRIPT_DIR/../../../gradlew)
export CDK_DISABLE_LEGACY_EXPORT_WARNING=1

cdklocal_deploy() {
	( 
		cd  $SCRIPT_DIR/../$1/stack
		cdklocal bootstrap
		cdklocal deploy $1-local --require-approval never
	)
}

(
	cd $SCRIPT_DIR/..
	$GW shadowJar
)

cdklocal_deploy "sut-event-hub"
cdklocal_deploy "sut-control-service"

(
	export POWERTOOLS_LOG_LEVEL=WARN
    export POWERTOOLS_SERVICE_NAME=payment
	cd $SCRIPT_DIR
	$GW integrationTest
)
