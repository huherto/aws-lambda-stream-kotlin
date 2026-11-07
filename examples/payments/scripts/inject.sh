#!/usr/bin/env bash
# inject.sh  Publishes internal events onto our bus, standing in for whichever gateway would produce them.
#
#   scripts/inject.sh submit p-7 --from C-1 --to K-2 --amount 100   # PUT /payments/p-7 on the BFF, as u-42
#   scripts/inject.sh withdrawn p-7                                 # as the Checking ESG
#   scripts/inject.sh withdrawal-failed p-7 --reason insufficient-funds
#   scripts/inject.sh deposited p-7                                 # as the Card or Bank ESG
#   scripts/inject.sh deposit-failed p-7 --reason account-closed
#   scripts/inject.sh refunded p-7
#   scripts/inject.sh view p-7                                      # GET /payments/p-7
#   scripts/inject.sh list                                          # GET /payments
#
# Destination accounts starting with K are cards, with B are banks.
# Injected events carry tags.source "inject", and their ids are <payment>-<type>, so running the same line
# twice shows idempotence: nothing changes the second time.

set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$SCRIPT_DIR/common.sh"

CUSTOMER="${CUSTOMER:-u-42}"

usage() {
	sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
	exit 1
}

[[ $# -ge 1 ]] || usage
COMMAND="$1"; shift
PAYMENT_ID=""
if [[ "$COMMAND" != "list" ]]; then
	[[ $# -ge 1 ]] || usage
	PAYMENT_ID="$1"; shift
fi

FROM="C-1"; TO="K-2"; AMOUNT="100"; CURRENCY="USD"; REASON=""
while [[ $# -gt 0 ]]; do
	case "$1" in
		--from) FROM="$2"; shift 2 ;;
		--to) TO="$2"; shift 2 ;;
		--amount) AMOUNT="$2"; shift 2 ;;
		--currency) CURRENCY="$2"; shift 2 ;;
		--reason) REASON="$2"; shift 2 ;;
		--customer) CUSTOMER="$2"; shift 2 ;;
		*) echo "Unknown option $1" >&2; usage ;;
	esac
done

# The payment as the BFF stored it, shaped as the events' payload.
payment_json() {
	local item
	item=$(awslocal dynamodb get-item --table-name "$BFF_TABLE" --consistent-read \
		--key "{\"pk\":{\"S\":\"$PAYMENT_ID\"},\"sk\":{\"S\":\"PAYMENT\"}}" --output json)
	if [[ -z "$item" || "$(echo "$item" | jq '.Item.requestHash')" == "null" ]]; then
		echo "Payment $PAYMENT_ID not found in $BFF_TABLE. Submit it first." >&2
		return 1
	fi
	echo "$item" | jq -c '.Item | {
		id: .pk.S, customerId: .customerId.S,
		from: {kind: .fromKind.S, account: .fromAccount.S},
		to: {kind: .toKind.S, account: .toAccount.S},
		amount: .amount.S, currency: .currency.S, submittedAt: (.submittedAt.N | tonumber)}'
}

# inject_event <TYPE> <id suffix> [reason]
inject_event() {
	local type="$1" suffix="$2" reason="$3"
	local payment detail
	payment=$(payment_json)
	detail=$(jq -n -c --arg type "$type" --arg id "$PAYMENT_ID-$suffix" --arg pk "$PAYMENT_ID" \
		--argjson ts "$(now_ms)" --argjson payment "$payment" --arg reason "$reason" \
		'{type: $type, id: $id, timestamp: $ts, partitionKey: $pk, tags: {source: "inject"}, payment: $payment}
		 + (if $reason == "" then {} else {reason: $reason} end)')
	put_event "inject" "$type" "$detail"
}

case "$COMMAND" in
	submit)
		case "$TO" in
			K-*) TO_KIND="card" ;;
			B-*) TO_KIND="bank" ;;
			*) echo "--to must start with K (card) or B (bank)" >&2; exit 1 ;;
		esac
		BODY=$(jq -n -c --arg from "$FROM" --arg to "$TO" --arg kind "$TO_KIND" --arg amount "$AMOUNT" --arg cur "$CURRENCY" \
			'{from: {kind: "checking", account: $from}, to: {kind: $kind, account: $to}, amount: $amount, currency: $cur}')
		curl -s -w '\nHTTP %{http_code}\n' -X PUT "$(bff_url)/payments/$PAYMENT_ID" \
			-H "Content-Type: application/json" -H "X-Customer-Id: $CUSTOMER" -d "$BODY"
		;;
	withdrawn) inject_event "FUNDS_WITHDRAWN" "funds-withdrawn" ;;
	withdrawal-failed) inject_event "WITHDRAWAL_FAILED" "withdrawal-failed" "${REASON:-insufficient-funds}" ;;
	deposited) inject_event "FUNDS_DEPOSITED" "funds-deposited" ;;
	deposit-failed) inject_event "DEPOSIT_FAILED" "deposit-failed" "${REASON:-account-closed}" ;;
	refunded) inject_event "FUNDS_REFUNDED" "funds-refunded" ;;
	view)
		curl -s -w '\nHTTP %{http_code}\n' "$(bff_url)/payments/$PAYMENT_ID" -H "X-Customer-Id: $CUSTOMER"
		;;
	list)
		curl -s -w '\nHTTP %{http_code}\n' "$(bff_url)/payments" -H "X-Customer-Id: $CUSTOMER"
		;;
	*) usage ;;
esac
