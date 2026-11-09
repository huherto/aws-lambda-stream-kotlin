#!/usr/bin/env bash
# publish-accounts.sh  Plays the accounts subsystem: publishes one ACCOUNTS.ACCOUNT_OPENED per seed account
# onto our bus with source "external", as the accounts subsystem's egress rule would. Our accounts ingress
# gateway turns each into ACCOUNT_CHANGED for the BFF. Run it once after deploying, before the first payment.
#
# Usage:
#   scripts/publish-accounts.sh            # the seed accounts
#   scripts/publish-accounts.sh --rename   # ACCOUNTS.ACCOUNT_UPDATED with both holderId and ownerId:
#                                          # the first of the accounts team's three deployments.
#
# Ids and timestamps are fixed, so running it twice changes nothing.

set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$SCRIPT_DIR/common.sh"

# number customer status timestamp
SEED_ACCOUNTS=(
	"C-1 u-42 open 1730000000000"    # the running example's source: everything succeeds
	"C-2 u-42 open 1730000000000"    # in simulation, withdrawals are rejected: insufficient funds
	"C-7 u-42 open 1730000000000"    # fault account: core checking applies the debit, but the call times out
	"C-3 u-77 open 1730000000000"    # another customer's account: u-42 can't use it
	"C-9 u-42 closed 1730000000000"  # closed: payments from it get 400
)

external_event() {
	local type="$1" id="$2" number="$3" customer="$4" status="$5" timestamp="$6" customer_fields="$7"
	jq -n -c --arg type "$type" --arg id "$id" --arg n "$number" --arg c "$customer" --arg s "$status" \
		--argjson ts "$timestamp" --argjson fields "$customer_fields" \
		'{id: $id, type: $type, timestamp: $ts, partitionKey: $n,
		  tags: {source: "accounts-egress-gateway", subsystem: "accounts"},
		  account: ({number: $n, kind: "checking", status: $s, currency: "USD"} + ($fields | map({(.): $c}) | add))}'
}

if [[ "$1" == "--rename" ]]; then
	for seed in "${SEED_ACCOUNTS[@]}"; do
		read -r number customer status timestamp <<< "$seed"
		detail=$(external_event "ACCOUNTS.ACCOUNT_UPDATED" "acct-$number-renamed" "$number" "$customer" "$status" \
			$((timestamp + 1000)) '["holderId","ownerId"]')
		put_event "external" "ACCOUNTS.ACCOUNT_UPDATED" "$detail"
	done
	exit 0
fi

for seed in "${SEED_ACCOUNTS[@]}"; do
	read -r number customer status timestamp <<< "$seed"
	detail=$(external_event "ACCOUNTS.ACCOUNT_OPENED" "acct-$number-opened" "$number" "$customer" "$status" \
		"$timestamp" '["holderId"]')
	put_event "external" "ACCOUNTS.ACCOUNT_OPENED" "$detail"
done
