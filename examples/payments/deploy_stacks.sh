#!/usr/bin/env bash
# deploy_stacks.sh Deploy one or more payments stacks to localstack.
#
# Usage:
#   ./deploy_stacks.sh                       # test, build and deploy everything
#   ./deploy_stacks.sh payments-bff          # only these stacks
#   ./deploy_stacks.sh -d                    # delete localstack state first
#
# After the first deploy, run scripts/publish-accounts.sh: the BFF only accepts payments
# from accounts it has heard of through ACCOUNT_CHANGED.

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GW=$(realpath "$SCRIPT_DIR/../../gradlew")
export CDK_DISABLE_LEGACY_EXPORT_WARNING=1

# The logical order to deploy the stacks (PaymentsServiceSpecs.md › Services and deploy order).
# Each external system's ingress gateway goes before its egress gateway: in simulation mode the egress gateway
# delivers responses to the ingress gateway's input. downstream-event-hub goes before payments-egress-gateway,
# whose egress rule targets it.
STACKS=(
	"payments-event-hub"
	"payments-event-fault-monitor"
	"payments-event-lake"
	"payments-control-service"
	"payments-bff"
	"payments-core-checking-ingress-gateway"
	"payments-core-checking-egress-gateway"
	"payments-card-processor-ingress-gateway"
	"payments-card-processor-egress-gateway"
	"payments-bank-network-ingress-gateway"
	"payments-bank-network-egress-gateway"
	"downstream-event-hub"
	"payments-egress-gateway"
	"payments-accounts-ingress-gateway"
)

usage() {
	echo "Usage: $0 [-d] [stack ...]"
	echo
	echo "Options:"
	echo "  -d  Delete localstack state before deploying"
	echo
	echo "Available stacks:"
	printf '  %s\n' "${STACKS[@]}"
}

is_known_stack() {
	local stack="$1"
	for known_stack in "${STACKS[@]}"; do
		if [[ "$stack" == "$known_stack" ]]; then
			return 0
		fi
	done
	return 1
}

cdklocal_deploy() {
	(
		cd "$SCRIPT_DIR/$1/infra"
		cdklocal bootstrap
		cdklocal deploy "$1-local" --require-approval never
	)
}

DELETE_LOCALSTACK_STATE=false
ARGS=()
for arg in "$@"; do
	if [[ "$arg" == "-d" ]]; then
		DELETE_LOCALSTACK_STATE=true
	elif [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
		usage
		exit 0
	else
		ARGS+=("$arg")
	fi
done

if [[ "${#ARGS[@]}" -eq 0 ]]; then
	STACKS_TO_DEPLOY=("${STACKS[@]}")
else
	STACKS_TO_DEPLOY=("${ARGS[@]}")
fi

for stack in "${STACKS_TO_DEPLOY[@]}"; do
	if ! is_known_stack "$stack"; then
		echo "Unknown stack: $stack" >&2
		echo >&2
		usage >&2
		exit 1
	fi
done

(
	cd "$SCRIPT_DIR"
	$GW test shadowJar
)

if [[ "$DELETE_LOCALSTACK_STATE" == true ]]; then
	curl -X DELETE http://localhost:4566/_localstack/state
fi

for stack in "${STACKS[@]}"; do
	for requested_stack in "${STACKS_TO_DEPLOY[@]}"; do
		if [[ "$stack" == "$requested_stack" ]]; then
			cdklocal_deploy "$stack"
			break
		fi
	done
done
