#!/usr/bin/env bash
# Shared settings for the payments demo scripts. Requires awslocal, jq and curl.

STAGE="${STAGE:-local}"
BUS_NAME="payments-event-hub-${STAGE}-bus"
BFF_TABLE="payments-bff-${STAGE}-payments"
BFF_API_NAME="payments-bff-${STAGE}-api"
LOCALSTACK_URL="${LOCALSTACK_URL:-http://localhost:4566}"

now_ms() {
	# macOS date has no %N; python3 is available on macOS and Linux.
	python3 -c 'import time; print(int(time.time() * 1000))'
}

bff_url() {
	local api_id
	api_id=$(awslocal apigateway get-rest-apis --query "items[?name=='${BFF_API_NAME}'].id" --output text)
	if [[ -z "$api_id" || "$api_id" == "None" ]]; then
		echo "BFF API ${BFF_API_NAME} not found. Is payments-bff deployed?" >&2
		return 1
	fi
	echo "${LOCALSTACK_URL}/restapis/${api_id}/${STAGE}/_user_request_"
}

# put_event <bus source> <detail type> <detail json>
put_event() {
	local source="$1" detail_type="$2" detail="$3"
	local entries
	entries=$(jq -n --arg bus "$BUS_NAME" --arg src "$source" --arg dt "$detail_type" --arg d "$detail" \
		'[{EventBusName: $bus, Source: $src, DetailType: $dt, Detail: $d}]')
	awslocal events put-events --entries "$entries" --query 'FailedEntryCount' --output text | grep -q '^0$' \
		|| { echo "put-events failed for $detail_type" >&2; return 1; }
	echo "published $detail_type $(echo "$detail" | jq -r .id)"
}
