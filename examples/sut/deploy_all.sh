#!/usr/bin/env bash
# deploy_all.sh Deploy all the stacks to localstack.

set -e    # (errexit): Exit immediately if a command exits with a non-zero status.
# set -x  # (xtrace): Print each command to stdout before executing it (useful for debugging).

# SCRIPT_DIR is the directory where the script is located.
# All the operations are relative to this location regardless of where the
# script is executed.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GW=$(realpath $SCRIPT_DIR/../../gradlew)
export CDK_DISABLE_LEGACY_EXPORT_WARNING=1 # Diable annoying warning.

cdklocal_deploy() {
	( 
		cd  $SCRIPT_DIR/$1/infra
		cdklocal bootstrap
		cdklocal deploy $1-local --require-approval never
	)
}

(
  cd $SCRIPT_DIR/../..
  $GW test
	cd $SCRIPT_DIR
	$GW shadowJar
)

# There is logical order in which these should be deployed.
cdklocal_deploy "sut-event-hub"
cdklocal_deploy "sut-control-service"
cdklocal_deploy "sut-shipment-bff"
