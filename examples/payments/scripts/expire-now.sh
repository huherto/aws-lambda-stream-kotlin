#!/usr/bin/env bash
# expire-now.sh  Asks LocalStack to delete expired DynamoDB items now instead of at its next periodic sweep.
#
# The control service's timeout is a DynamoDB TTL: each DEPOSIT_REQUESTED writes a deadline item whose TTL is
# the deadline (2 minutes locally). Only when the TTL deletes it does PAYMENT_EXPIRED appear. LocalStack sweeps
# expired items periodically (with DYNAMODB_REMOVE_EXPIRED_ITEMS=1); run this after the deadline to see the
# timeout right away. In AWS, TTL deletion typically happens within minutes to hours of expiry.

set -e
LOCALSTACK_URL="${LOCALSTACK_URL:-http://localhost:4566}"
curl -s -X DELETE "$LOCALSTACK_URL/_aws/dynamodb/expired"
echo
