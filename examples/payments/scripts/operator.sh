#!/usr/bin/env bash
# operator.sh  The operator's tools for the payments subsystem on LocalStack.
#
#   scripts/operator.sh faults                              what failed today, where and why
#   scripts/operator.sh release-claim <payment> [PMT|RFND]  after checking core checking: mark sent, or release it
#   scripts/operator.sh resubmit <function> [--dry-run]     resubmit today's faults of a function
#
# The fault monitor stores every fault event in S3 with the original record; resubmitting re-invokes the function
# with that record. Fix the cause first: resubmitting a fault whose cause is still there just fails again.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GW=$(realpath "$SCRIPT_DIR/../../../gradlew")
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
exec "$GW" -q --console=plain :examples:payments:operator:run --args="$*"
