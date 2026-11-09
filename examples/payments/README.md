# Payments Example

An autonomous payment subsystem built with `aws-lambda-stream-kotlin`: a customer submits a payment through a BFF,
a control service runs the payment process as a saga (withdraw, deposit, refund, timeout), and gateways stand
between the subsystem and the outside world.

- [PaymentsServiceSpecs.md](PaymentsServiceSpecs.md): what each service consumes, produces and guarantees.
- [PaymentsMockSystemsSpecs.md](PaymentsMockSystemsSpecs.md): how to run it without the real outside world.

Running example: payment `p-7`, 100 from checking account `C-1` to card `K-2`, for customer `u-42`.

## What is built

| # | Service | Status | Pattern | Framework pieces |
|---|---|---|---|---|
| 1 | `payments-event-hub` | ✅ | Event hub | EventBridge bus, Kinesis `s1` (default) and `ingress` channels, archive, logs |
| 2 | `payments-event-fault-monitor` | ✅ | Fault monitor | Same as the `sut` example |
| 3 | `payments-event-lake` | ✅ | Event lake | Same as the `sut` example (Firehose → S3) |
| 4 | `payments-control-service` | ✅ | Control service | `CollectPipeline`, `CorrelatePipeline`, `EvaluatePipeline` |
| 5 | `payments-bff` | ✅ | BFF | REST API, `MaterializePipeline`, `CdcPipeline` |
| 6 | `payments-core-checking-ingress-gateway`, `…-egress-gateway` | ✅ | ESG (legacy): API call, CDC on the ledger | `ActionPipeline`, `TranslatePipeline`, `MaterializePipeline` |
| 7 | `payments-card-processor-ingress-gateway`, `…-egress-gateway` | ✅ | ESG (SaaS): API call, signed webhook | same, plus API Gateway |
| 8 | `payments-bank-network-ingress-gateway`, `…-egress-gateway` | ✅ | ESG (batch): files out, files in | same, plus S3 → SQS |
| 9 | `downstream-event-hub` | ✅ | Neighbour as configuration | A bus and a catch-all log |
| 10 | `payments-egress-gateway` | ✅ | ESG (inter-subsystem egress) + the egress rule | `CdcPipeline`, publisher with source `external` |
| 11 | `payments-accounts-ingress-gateway` | ✅ | ESG (inter-subsystem ingress) | `CdcPipeline` over Kinesis |

`scripts/inject.sh` can still play any gateway's part, for demos of the control service and the BFF alone.

## How the services work

### Event model (`common-app`)

Internal events are one sealed hierarchy, `PaymentsEvent`, encoded with the framework's `KotlinxEventCodec`.
Every payment event carries the whole `Payment` (event-carried state transfer); failures add a `reason`.
`ACCOUNT_CHANGED` carries an `Account` instead. Amounts are decimal strings (`"100.00"`).
Shared test fixtures for `p-7` (one per event type) live in `common-app/src/testFixtures`.

### Control service

```
default channel ─► listener: CollectPipeline ─► events table ─► (stream) ─► trigger
                                                                            ├─ CorrelatePipeline  pk = <payment>
                                                                            ├─ CorrelatePipeline  pk = <payment>.deadline, ttl = deadline
                                                                            └─ EvaluatePipeline × 7 (one per rule) ─► bus
```

- Each rule is an `EvaluatePipeline` that fires only on **correlation inserts**, filtered by the triggering type.
  A custom normalizer gives emitted events deterministic ids, `<payment>.<rule>` (`p-7.start`, `p-7.done`, …),
  so a rule that fires again emits the same id and consumers drop the repeat.
- **Timeout via DynamoDB TTL.** `DEPOSIT_REQUESTED` writes a deadline item (`p-7.deadline`, `expire = true`) whose
  TTL is the deadline: 2 minutes locally, 2 days elsewhere. When the TTL deletes it, the `timeout` rule queries the
  payment's events and emits `PAYMENT_EXPIRED` (dated at the deadline) if neither `FUNDS_DEPOSITED` nor
  `DEPOSIT_FAILED` was heard. A late deposit or failure still completes or refunds the payment.
- `undone` needs the `DEPOSIT_FAILED` behind a refund; without it the rule can't decide, and the unit of work
  becomes a `fault`.

### BFF

- `PUT /payments/{id}` validates at the edge (amount, destination, a known **open** checking account of the caller),
  then writes the payment with a condition, so a repeated `PUT` writes nothing. Same body → `202` with the stored
  view; different body → `409`.
- **Accepted means published:** `PAYMENT_SUBMITTED` (`<id>.submitted`) comes from the table's stream through a
  `CdcPipeline`, never from the request handler, so there is no dual write.
- The listener records `at_<TYPE>` (the event's own timestamp, first one wins) per heard event, and keeps
  accounts current from `ACCOUNT_CHANGED` (the latest event wins, by a condition on `timestamp`).
- `status` is computed on read from the set of heard events with the spec's first-match table, never from their
  order.
- Send bodies as `Content-Type: application/json`. A bare `curl -d` sends a form content type, and LocalStack's
  API Gateway then doesn't pass the body to the Lambda (`400 the body is required`).
- `X-Customer-Id` is a stand-in for an access token. **It is not authentication and must not ship.**

### External-system gateways and simulation mode

Each external system has an egress gateway (acts, publishes nothing) and an ingress gateway (hears back, publishes
internal events). Shared pieces live in `common-gateway`:

- `ActionPipeline` (egress) and `TranslatePipeline` (ingress, from raw records rather than events).
- Each system's own dialect, in one file per system: ledger rows (`REF_NO`, `AMT`, `STAT`, `REJ_CD`), signed
  webhooks (`event_id`, `amount_cents`, `failure_code`, `Card-Signature: t=…,v1=<HMAC-SHA256>`), and CSV files
  (`reference,status,code`).
- **Requests view.** An answer from outside carries only a reference, but our events carry the whole payment.
  So each ingress gateway materializes the request events it will hear back about and joins answers to them. An
  answer that arrives first throws `PaymentNotKnownYet`, which is retryable (`STREAM_RETRY_ENABLED=true`): the
  source redelivers instead of producing a fault. The webhook answers `503` so the processor retries.

| External system | Ingress input (owned by the ingress stack) | Ingress event id |
|---|---|---|
| Core checking | `ledger` table + stream (stands in for CDC on the real log) | `<ENTRY_ID>.<type>` |
| Card processor | `POST /webhooks/card-processor` (URL and secret published in SSM) | `<webhook event_id>.<type>` |
| Bank network | `inbound/` in an S3 bucket → SQS | `<file name>:<reference>.<type>` |

**Simulation mode** (`SIMULATE=true`) is on locally and off elsewhere (`-c simulate=…` overrides; it refuses
`prod`). The egress gateway doesn't call the system: it sends the system's response, in the system's format, to
an SQS queue with a delay. A simulator function then delivers it to the ingress gateway's real input: it writes the
ledger row, posts the signed webhook, or drops the settlement file. The ingress side can't tell it from the real
system.

| Account | Simulated outcome | Delay (local) |
|---|---|---|
| `C-2` | Withdrawal rejected (`NSF` → `insufficient-funds`) | 2 s |
| `K-3` | Card credit fails (`card_closed` → `account-closed`) | 5 s |
| `B-8` | Bank transfer rejected (`R03` → `account-not-found`) | 20 s |
| `B-7` | Bank transfer settles after the 2-minute deadline | 180 s |
| `C-5` | Withdrawal succeeds; core checking goes down for everyone for 60 s first (fault account) | 2 s after it's back |
| `C-8` | The first withdrawal call is lost; it becomes a fault an operator resolves (fault account) | after resubmission |
| `C-7` | Withdrawal succeeds; the call times out after the debit was applied (fault account) | 2 s |
| `K-6` | Card credit succeeds; the webhook is delivered twice (fault account) | 5 s, again 3 s later |
| `B-6` | Bank transfer settles; the settlement file is dropped twice (fault account) | 20 s, again 3 s later |
| any other | Success | 2 s / 5 s / 20 s |

The core checking simulation numbers every posting (`ENTRY_ID`) the way core checking does, so a second call is a
second debit, visible in the ledger. Fault accounts (`K-6`, `B-6`) succeed but make the external system answer twice.

**Called once (core checking).** Core checking has no idempotency keys, so the egress gateway never calls twice
blindly:
1. It claims each request (`<payment>#PMT` or `#RFND`) in its `calls` table before calling, and marks it `SENT` after.
2. A repeat that finds `SENT` does nothing.
3. A call whose outcome is unknown leaves the claim `CALLING`, with the request kept in it: the request is *in
   doubt*. It doesn't wait on the channel, so it never holds up other payments' withdrawals.
4. Every minute a **reconciler** redelivers each request in doubt to the listener, which asks core checking once
   whether the posting exists (in simulation, the ledger's `byReference` index):
   - found: it marks the request `SENT`, without calling again;
   - not found yet: it stays in doubt until the next round;
   - still not found after the grace period (5 minutes; 1 minute locally): it becomes a `fault` for an operator.
5. A call that certainly never left (connection refused) releases the claim, so the next attempt calls normally.

**Circuit breaker (core checking).** Breaker state is one item in the egress gateway's `calls` table:
- **Closed:** three failures in a row (timeouts, refused connections, errors; not rejections) open it.
- **Open:** for 15 s locally (30 s elsewhere), no calls and no inquiries. An invocation sleeps until the trial time
  (at most 20 s), then hands the request back to the stream, which keeps it in order.
- **Half-open:** exactly one request is the trial; success closes the breaker, failure reopens it.
- **Closed hours:** `-c coreCheckingClosedHours=22:00-06:00@America/New_York` keeps it open without any failure.
- **Grace period:** a request in doubt gets its full grace period counted from when core checking came back.

**Rate limit (card processor).** Every call takes a token from a per-second budget in the gateway's `rate` table,
shared by all instances; with none left, it waits for the next second, so the backlog waits on the stream. A `429`
that slips through is waited out (`Retry-After`) and retried, which is safe because the payment id is the idempotency
key. The simulated processor always enforces the same rate (5/s locally, `-c cardProcessorRate=…` elsewhere) and counts
its `429`s.

The real-mode connectors (`HttpCoreChecking`, `HttpCardProcessor`, `S3TransferFile`) exist but are not deployed
here. Not built: leasing rate tokens in blocks for high rates, and the nightly job that joins the transfer lines and sends the file at cut-off.

### Payments egress gateway and the downstream hub

`PAYMENT_COMPLETED` and `PAYMENT_FAILED` become `PAYMENTS.PAYMENT_COMPLETED` and `PAYMENTS.PAYMENT_FAILED`
(id `<internal id>.external`), published with the bus source `external`. The payload is curated: no `raw`, no
`triggers`, no account numbers, and reasons only from the public list. The stack's **egress rule**
(`source = external`, type prefix `PAYMENTS.`) forwards them to `downstream-event-hub-<stage>-bus`, whose catch-all
rule logs them to `/aws/events/downstream-event-hub-local-events`. The contract is published as JSON Schema in
[`payments-egress-gateway/schema`](payments-egress-gateway/schema/payments-external-events.schema.json).

### Accounts ingress gateway

Reads the `ingress` channel (`source = external`, `ACCOUNTS.*`), translates to `ACCOUNT_CHANGED`
(`holderId` or `ownerId` → `customerId`), keeps the external event unchanged in `raw`, and ignores unknown types.

## Running it locally

Prerequisites, as for the `sut` example: Docker, LocalStack, `awslocal`, `cdklocal`, `jq`, `curl`, `python3`.

```bash
cd examples/payments
docker compose up -d

./deploy_stacks.sh -d                # tests, builds the jars, deploys all built stacks
scripts/publish-accounts.sh          # once: the BFF only accepts accounts it has heard of
```

### Teaching scenarios with simulation (the gateways do everything)

```bash
scripts/inject.sh submit s-card         --to K-2          # → COMPLETED within ~10 s
scripts/inject.sh submit s-card-closed  --to K-3          # → refund → FAILED (account-closed)
scripts/inject.sh submit s-nsf          --from C-2        # → FAILED (insufficient-funds), nothing to undo
scripts/inject.sh submit s-bank         --to B-9          # → COMPLETED after ~25 s
scripts/inject.sh submit s-bank-unknown --to B-8          # → refund → FAILED (account-not-found)
scripts/inject.sh submit s-bank-late    --to B-7          # → EXPIRED after 2 min (run scripts/expire-now.sh),
                                                          #   then COMPLETED when the late settlement arrives
scripts/inject.sh view s-bank-late

# What crossed the boundary: only PAYMENTS.*, no raw, no account numbers, no PAYMENT_EXPIRED
awslocal logs filter-log-events --log-group-name /aws/events/downstream-event-hub-local-events
```

### Teaching scenarios with `inject`

With the gateways deployed in simulation mode they answer every request themselves, so injected events would
duplicate theirs. To drive payments by hand, deploy without the six external-system gateways:
`./deploy_stacks.sh -d payments-event-hub payments-event-fault-monitor payments-event-lake payments-control-service payments-bff payments-accounts-ingress-gateway`.

```bash
# Happy path
scripts/inject.sh submit p-7 --from C-1 --to K-2 --amount 100
scripts/inject.sh withdrawn p-7      # → the control service emits DEPOSIT_REQUESTED
scripts/inject.sh deposited p-7      # → PAYMENT_COMPLETED
scripts/inject.sh view p-7

# Refund (compensation)
scripts/inject.sh submit p-8 --to K-3
scripts/inject.sh withdrawn p-8
scripts/inject.sh deposit-failed p-8 --reason account-closed   # → REFUND_REQUESTED
scripts/inject.sh refunded p-8                                 # → PAYMENT_FAILED (account-closed)

# Not enough money: nothing to undo
scripts/inject.sh submit p-9 --from C-2
scripts/inject.sh withdrawal-failed p-9 --reason insufficient-funds

# Late deposit: timeouts aren't outcomes
scripts/inject.sh submit p-10 --to B-7
scripts/inject.sh withdrawn p-10
sleep 130 && scripts/expire-now.sh   # → PAYMENT_EXPIRED, status EXPIRED
scripts/inject.sh deposited p-10     # → PAYMENT_COMPLETED

# Idempotence: run any inject line twice; nothing changes the second time.
# Contract change: only the accounts ingress gateway notices.
scripts/publish-accounts.sh --rename
```

`scripts/expire-now.sh` does now what DynamoDB TTL does eventually: it deletes the control service's deadline items
whose deadline has passed, and the control service reacts as it would to a TTL deletion. It also asks LocalStack to
sweep (`DYNAMODB_REMOVE_EXPIRED_ITEMS=1` in `docker-compose.yml`), but that sweep stopped deleting items from our tables
after a few state resets, so the script doesn't rely on it.

Every event on the bus is logged to `/aws/events/payments-event-hub-local-events` (faults to `…-faults`):
`awslocal logs filter-log-events --log-group-name /aws/events/payments-event-hub-local-events`.

## Faults and resubmission: an operator walkthrough

A fault is an event: a unit of work that failed for a reason retrying won't fix. It goes to the faults channel, and
the fault monitor stores it in S3 (`myorg-payments-event-fault-monitor-local-us-east-1`) with the original record, and
sends an alert to its SNS topic. Nothing else happens until someone acts on it. That's the operator's job:

```bash
scripts/inject.sh submit p-lost --from C-8 --to K-2   # C-8: the first debit call is lost on the way

# About a minute later (the grace period locally), the request in doubt becomes a fault:
scripts/operator.sh faults
#   2026-11-09T22:30:05Z  payments-core-checking-egress-gateway-local-listener [withdraw]
#       p-lost#PMT: in doubt since … and core checking shows no posting; check it before retrying

# Fix first: the operator checks core checking. No posting, so the request was never applied: release it.
scripts/operator.sh release-claim p-lost
#   p-lost#PMT (CALLING): core checking shows no posting; claim released, safe to resubmit
# (Had core checking shown the posting, release-claim would mark the request sent instead: no second debit.)

# Then resubmit the function's faults: the original record goes to the original function.
scripts/operator.sh resubmit payments-core-checking-egress-gateway-local-listener --dry-run
scripts/operator.sh resubmit payments-core-checking-egress-gateway-local-listener

scripts/inject.sh view p-lost                          # → COMPLETED, debited once
```

- **Fix first.** Resubmitting without fixing the cause just fails again; here, the claim would still be in doubt.
  "Fix first" also means core checking is up: `operator.sh resubmit` refuses while its breaker isn't closed.
- **Every resubmission is checked.** A resubmission is a direct invocation: no stream retries it, and a function that
  throws still answers `200` (the failure is in `FunctionError`). `operator.sh resubmit` checks each outcome, retries
  with back-off, and exits `1` listing whatever still fails.
- **Resubmitting is safe to repeat.** The fault monitor may store a fault more than once (its delivery is at least
  once), and the resubmit tool doesn't deduplicate. Every consumer is idempotent, so a repeat changes nothing.
- `scripts/operator.sh` runs the `operator` module, which uses the framework's `ResubmitFaults` with
  LocalStack-aware clients.

## Integration tests

`integration-test` checks the deployed subsystem end to end on LocalStack, through its real entry points: HTTP
to the BFF via API Gateway, `ACCOUNTS.*` events onto our bus, and simulation mode for every external system.
Nothing is injected.

```bash
./run_itests.sh                                    # all of them, about 4 minutes
./run_itests.sh 'org.myorg.payments.BffApiITest'   # one class
```

| Class | What it proves |
|---|---|
| `SimulatedPaymentsITest` | The six simulated outcomes, each step coming from the gateway that heard it; the late deposit expires at the deadline and then completes |
| `BffApiITest` | `PUT` once (`202`, repeat `202`, other body `409`), validation (`401`, `400` with the field), `404` for another customer's payment, newest-first paging and status filter, latest `ACCOUNT_CHANGED` wins |
| `BoundaryITest` | Only curated `PAYMENTS.*` events with source `external` reach the downstream bus: no `raw`, no `triggers`, no account numbers, no `ACCOUNTS.*` forwarded |
| `IdempotenceITest` | A redelivered webhook event and a retried `PUT` change nothing |
| `RateLimitITest` | A burst of 20 card payments is paced at the processor's 5/s: all complete, no second sees more than 5 credits, and the simulated processor counts no `429` |
| `CircuitBreakerITest` | During a core checking outage (`C-5`) the breaker opens; payments wait, then complete, each debited once |
| `FaultResubmissionITest` | A lost request (`C-8`) becomes a fault, stored and alerted; checked, released and resubmitted, the payment completes, debited once |
| `CalledOnceITest` | Core checking is debited once, even when its answer is lost (`C-7`) or a `WITHDRAWAL_REQUESTED` is delivered twice; checked in the ledger |
| `DuplicatesITest` | Fault accounts: a webhook delivered twice (`K-6`) and a settlement file dropped twice (`B-6`) still give one deposit, one outcome and one external event |

- **Isolated per run:** each run uses its own payment ids, customers and accounts, so runs don't interfere and
  the list endpoint sees exactly the run's payments.
- **Reading the boundary:** the downstream hub has a verification queue next to its log, so the tests can read
  what crossed.
- **Accounts first:** the default channel has one shard, so the tests publish all their accounts before any
  payment. Otherwise the accounts would queue behind the payments' events.
