#!/usr/bin/env bash
# run_itests.sh  Runs the payments integration tests against the stacks deployed on LocalStack.
#
# Usage:
#   ./run_itests.sh                                   # every test (about 4 minutes: the late deposit waits for its deadline)
#   ./run_itests.sh 'org.myorg.payments.BffApiITest'  # a Gradle test filter
#
# Needs: docker compose up, ./deploy_stacks.sh, and the seed accounts (the tests publish them if missing).

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GW=$(realpath "$SCRIPT_DIR/../../gradlew")
source "$SCRIPT_DIR/scripts/common.sh"

cd "$SCRIPT_DIR"

PAYMENTS_BFF_URL=$(bff_url) || exit 1
export PAYMENTS_BFF_URL
echo "BFF: $PAYMENTS_BFF_URL"

TASK=":examples:payments:integration-test:integrationTest"

# On macOS, keep the machine awake: if it idles to sleep mid-run, everything (LocalStack included) stops.
AWAKE=""
command -v caffeinate > /dev/null && AWAKE="caffeinate -i"

if [[ -n "$1" ]]; then
	(set -x; $AWAKE $GW "$TASK" --tests "$1")
else
	(set -x; $AWAKE $GW "$TASK")
fi
