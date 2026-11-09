#!/usr/bin/env bash
# expire-now.sh  Does now what DynamoDB TTL would do eventually: deletes the control service's deadline items whose
# deadline has passed, so PAYMENT_EXPIRED appears right away.
#
# The control service's timeout is a DynamoDB TTL: each DEPOSIT_REQUESTED writes a deadline item (`expire = true`) whose
# `ttl` is the deadline (2 minutes locally). When the item is deleted, the control service checks whether the deposit
# is late. In AWS, TTL deletes expired items within minutes to hours. LocalStack sweeps periodically (with
# DYNAMODB_REMOVE_EXPIRED_ITEMS=1), but its sweep has proved unreliable after state resets, so this script also deletes
# the expired deadline items itself. A deletion is a deletion: the control service can't tell, and doesn't need to.

set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$SCRIPT_DIR/common.sh"

TABLE="payments-control-service-${STAGE}-events"
NOW=$(python3 -c 'import time; print(int(time.time()))')

# 1. Ask LocalStack to sweep (harmless if it works, ignored if it doesn't).
curl -s -X DELETE "$LOCALSTACK_URL/_aws/dynamodb/expired" > /dev/null || true

# 2. Delete the expired deadline items ourselves.
KEYS_FILE=$(mktemp)
awslocal dynamodb scan --table-name "$TABLE" \
	--filter-expression "#e = :t AND #ttl < :now" \
	--expression-attribute-names '{"#e":"expire","#ttl":"ttl"}' \
	--expression-attribute-values "{\":t\":{\"BOOL\":true},\":now\":{\"N\":\"$NOW\"}}" \
	--projection-expression "pk, sk" --output json \
	| jq -c '.Items[]' > "$KEYS_FILE"

COUNT=0
while read -r key; do
	awslocal dynamodb delete-item --table-name "$TABLE" --key "$key"
	echo "expired $(echo "$key" | jq -r '.pk.S')"
	COUNT=$((COUNT + 1))
done < "$KEYS_FILE"
rm -f "$KEYS_FILE"

echo "{\"ExpiredDeadlines\": $COUNT}"
