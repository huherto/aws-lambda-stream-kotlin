# Payments Service Specifications

*One spec per autonomous service in the final design of [JugPresentation_Outline.md](JugPresentation_Outline.md#final-design--an-autonomous-payment-subsystem): what each one consumes, produces and exposes, and how it behaves seen from outside. Built from the outline's event table and the book's rules for events, idempotence and the service patterns.*

**Running example:** payment `p-7`, 100 from checking account `C-1` to card `K-2`.
**Load:** 10,000 payments/min normally, 200,000/min at peak (about 3,300/s, about 10,000 events/s on the bus).
**What's the book's and what's ours:** the envelope, the `fault` event, the service patterns (BFF, control service, ESG, event hub), deterministic ids, PUT with client-generated ids and the split between **internal and external domain events** are the book's. The payment fields, event names (internal and external), REST paths, reason codes and the Bank ESG's file handling are **ours**, designed for the talk. Each section's *Sources* line points to the book.
**Every service is a black box.** The spec says what goes in, what comes out, what can be observed and what is guaranteed; never how a service is built inside (its functions, tables, keys or timers). No products are named. The implementation will **not** use the book's TypeScript libraries; wherever the book names one of its own tools, read it as "the equivalent in our stack".

## Contents

| Service | Pattern | Actor it answers to | REST API | Consumes | Produces |
|---|---|---|---|---|---|
| [Payment BFF](#1-payment-bff) | BFF | The customer | Yes, for the customer app | 11 | 1 |
| [Payment control service](#2-payment-control-service) | Control service | The business (the payment process) | **None** | 6 | 6 |
| [Checking ESG](#3-checking-esg) | ESG (legacy) | Core checking | None | 2 | 3 |
| [Card ESG](#4-card-esg) | ESG (third-party SaaS) | Card processor | Webhook callback only | 1 | 2 |
| [Bank ESG](#5-bank-esg) | ESG (batch network) | Other banks | None (file drop) | 1 | 2 |
| [Event hub](#6-event-hub) | Event hub | Everyone (infrastructure) | None | all | none (routes) |
| [Payments egress ESG](#7-payments-egress-esg) | ESG (inter-subsystem, egress) | Downstream subsystems | None | 2 internal | 2 **external** |
| [Accounts ingress ESG](#8-accounts-ingress-esg) | ESG (inter-subsystem, ingress) | The accounts subsystem | None | 3 **external** | 1 internal |

### Services and deploy order

Subsystem prefix `payments`, like the existing example's `sut`. One folder and one stack per service. Gateways follow the book's naming convention, `<external-system>-ingress|egress-<capability>-gateway` (Ch. 7 › [Packaging](Chapter_07_Bridging_Intersystem_Gaps.md#packaging)), with **separate ingress and egress gateways** for every external system.

| # | Service | Spec section | Account |
|---|---|---|---|
| 1 | `payments-event-hub` | [6](#6-event-hub) | System |
| 2 | `payments-event-fault-monitor` | [6](#6-event-hub) (as in the existing example) | System |
| 3 | `payments-event-lake` | [6](#6-event-hub) (as in the existing example) | System |
| 4 | `payments-control-service` | [2](#2-payment-control-service) | System |
| 5 | `payments-bff` | [1](#1-payment-bff) | System |
| 6 | `payments-core-checking-ingress-gateway`, then `payments-core-checking-egress-gateway` | [3](#3-checking-esg) | System |
| 7 | `payments-card-processor-ingress-gateway`, then `payments-card-processor-egress-gateway` | [4](#4-card-esg) | System |
| 8 | `payments-bank-network-ingress-gateway`, then `payments-bank-network-egress-gateway` | [5](#5-bank-esg) | System |
| 9 | `downstream-event-hub` (the downstream subsystem's bus and log) | [Mocks › 4](PaymentsMockSystemsSpecs.md#4-neighbouring-subsystems-as-configuration) | System |
| 10 | `payments-egress-gateway` (owns the **egress rule**) | [7](#7-payments-egress-esg) | System |
| 11 | `payments-accounts-ingress-gateway` | [8](#8-accounts-ingress-esg) | System |

- Each external system's ingress gateway is deployed before its egress gateway: in [simulation mode](#simulation-mode-egress-esgs) the egress gateway delivers responses to the ingress gateway's input.
- The real external systems (core checking, card processor, bank network) live in other accounts and are not deployed here.
- `downstream-event-hub` comes before `payments-egress-gateway`, whose egress rule targets it. The event hub doesn't depend on any neighbour.
- Not deployed: the `inject` and `publish-accounts` scripts ([mocks](PaymentsMockSystemsSpecs.md#2-event-injection)). The regional health check is left for later.

**Before the first payment:** run `publish-accounts` ([mocks › 4](PaymentsMockSystemsSpecs.md#accounts-subsystem-upstream-a-publish-script)) once everything is deployed. The BFF only accepts payments from accounts it has heard of through `ACCOUNT_CHANGED`, so until then every `PUT` gets `400`.

---

## Shared contract

### Event envelope

Every event carries the book's standard envelope (Ch. 4 › [Event envelope](Chapter_04_Trusting_Facts_and_Eventual_Consistency.md#event-envelope)). The bus routes on these fields, consumers filter on them, and the lake stores by them.

| Field | Meaning | In this system |
|---|---|---|
| `id` | Unique, **deterministic** event id | **The same fact always gets the same id**, however often it's produced: a retried request, a redelivered webhook or a reprocessed file line yields the same id again |
| `type` | What happened | Internal: `PAYMENT_SUBMITTED`, `FUNDS_WITHDRAWN`, … External: the same shape with a **namespace prefix**, `PAYMENTS.PAYMENT_COMPLETED` (tables below). Upper snake case, like the existing example the app is modelled on. The bus routes on the type too. The one exception is `fault`, which keeps the framework's own lowercase name |
| `timestamp` | Epoch ms when the actor acted | The time of the fact, not the time we heard it |
| `partitionKey` | Groups related events on one shard | **The payment id** (`p-7`), so a payment's events stay together. Account events use the account number |
| `tags` | Source, function name, stage, region | `source: payments-card-processor-ingress-gateway`, …: always the producing service |
| `payment` | The payload: the entity's state (event-carried state transfer) | See [Payment payload](#payment-payload). Account events carry `account` instead |
| `raw` | The outside system's original data, unchanged | **Internal events only**, from ESG ingress: the webhook body, the CDC row, the file line. Never in an external event |
| `eem` | Envelope encryption metadata | Only if an event carries sensitive data (not in this talk) |

**Marking external events on the bus.** The book's egress ESGs "set the `source` field to `external` to simplify the routing rules" (Ch. 7 › [Egress – upstream subsystem](Chapter_07_Bridging_Intersystem_Gaps.md#egress--upstream-subsystem)). Here that is the **bus's own routing attribute for the source**, outside the event body: `external` for external events, the producing service for internal ones. `tags.source` always names the producing service, so the lake still shows who published what.

### Payment payload

Ours. Every payment event carries the payment's state at that moment, so a consumer never has to call back for it. Facts about one particular step, such as why it failed, go on the event, not in the payment.

```json
{
  "id": "p-7",
  "customerId": "u-42",
  "from":   { "kind": "checking", "account": "C-1" },
  "to":     { "kind": "card", "account": "K-2" },
  "amount": "100.00",
  "currency": "USD",
  "submittedAt": 1730900520000
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | string | yes | Generated by the client (`PUT /payments/{id}`) |
| `customerId` | string | yes | From the caller's identity (for now the `X-Customer-Id` header, see the [REST API](#rest-api)), never from the request body |
| `from` | `{ kind, account }` | yes | `kind` is always `checking` for now |
| `to` | `{ kind, account }` | yes | `kind` is `card` or `bank`. The Card and Bank ESGs filter on it |
| `amount` | **decimal as a string**, two decimals: `"100.00"` | yes | Exact and readable. Never a binary floating-point number |
| `currency` | ISO 4217 code | yes | |
| `submittedAt` | epoch ms | yes | |

### Event fields

The envelope fields (above) plus:

| Event | Carries | Its own fields |
|---|---|---|
| `PAYMENT_SUBMITTED`, `WITHDRAWAL_REQUESTED`, `FUNDS_WITHDRAWN`, `DEPOSIT_REQUESTED`, `FUNDS_DEPOSITED`, `REFUND_REQUESTED`, `FUNDS_REFUNDED`, `PAYMENT_COMPLETED`, `PAYMENT_EXPIRED` | `payment` | none |
| `WITHDRAWAL_FAILED`, `DEPOSIT_FAILED`, `PAYMENT_FAILED` | `payment` | `reason` (required): `insufficient-funds` · `account-closed` · `account-not-found` · `rejected` |
| `ACCOUNT_CHANGED` | `account` (not `payment`) | none. `account` is `{ number, kind, customerId, status }`, `status` `open` or `closed` |
| `fault` | the failed unit of work | the framework's fault format |

- `PAYMENT_FAILED` copies the `reason` of the failure that caused it.
- What an outside system actually said (the ledger row, the webhook body, the file line) stays in `raw`, never in typed fields.

### Event catalog

Two contracts, as the book prescribes (Ch. 2 › [External domain events](Chapter_02_Defining_Boundaries_and_Letting_Go.md#external-domain-events); Ch. 4 › [Internal versus external](Chapter_04_Trusting_Facts_and_Eventual_Consistency.md#internal-versus-external)):

- **Internal domain events** flow only between the services of this subsystem. They can change more freely and may carry `raw` and audit detail: "all in the family".
- **External domain events** are what this subsystem shares with others, and what it hears from them. They are stable contracts with strong backward compatibility, and only ESGs produce or translate them.

#### Internal events

| Event | Kind | Producer | Consumers |
|---|---|---|---|
| `PAYMENT_SUBMITTED` | Fact (starts the process) | Payment BFF | Control service |
| `WITHDRAWAL_REQUESTED` | Entry | Control service | Checking ESG |
| `FUNDS_WITHDRAWN` | Exit | Checking ESG | Control service, BFF |
| `WITHDRAWAL_FAILED` | Abort | Checking ESG | Control service, BFF |
| `DEPOSIT_REQUESTED` | Entry | Control service | Card ESG (`to.kind = card`), Bank ESG (`to.kind = bank`), BFF |
| `FUNDS_DEPOSITED` | Exit | Card ESG or Bank ESG | Control service, BFF |
| `DEPOSIT_FAILED` | Abort | Card ESG or Bank ESG | Control service, BFF |
| `REFUND_REQUESTED` | Entry (compensation) | Control service | Checking ESG, BFF |
| `FUNDS_REFUNDED` | Exit (compensation) | Checking ESG | Control service, BFF |
| `PAYMENT_COMPLETED` | Outcome | Control service | BFF, payments egress ESG |
| `PAYMENT_FAILED` | Outcome | Control service | BFF, payments egress ESG |
| `PAYMENT_EXPIRED` | Timeout, **not an outcome** | Control service | BFF. Stays **internal**; what else reacts to it is open (decision 9) |
| `ACCOUNT_CHANGED` | Reference data | Accounts ingress ESG | BFF |
| `fault` | Error | Any service | Fault monitor (outside this talk) |

No other subsystem consumes these. Anyone outside gets the external events below.

#### External events

| Event | Direction | Producer | Consumers | Translated from / to |
|---|---|---|---|---|
| `PAYMENTS.PAYMENT_COMPLETED` | Out | Payments egress ESG | Any downstream subsystem | From `PAYMENT_COMPLETED` |
| `PAYMENTS.PAYMENT_FAILED` | Out | Payments egress ESG | Any downstream subsystem | From `PAYMENT_FAILED` |
| `ACCOUNTS.ACCOUNT_OPENED` | In | Accounts subsystem | Accounts ingress ESG | To `ACCOUNT_CHANGED` |
| `ACCOUNTS.ACCOUNT_UPDATED` | In | Accounts subsystem | Accounts ingress ESG | To `ACCOUNT_CHANGED` |
| `ACCOUNTS.ACCOUNT_CLOSED` | In | Accounts subsystem | Accounts ingress ESG | To `ACCOUNT_CHANGED` |

- Only outcomes go out. `PAYMENT_EXPIRED` is a fact about our own timer and stays inside, for now.
- External payloads are curated: see the [payments egress ESG](#7-payments-egress-esg). The `ACCOUNTS.*` payload is the accounts subsystem's contract, not ours: see the [accounts ingress ESG](#8-accounts-ingress-esg).

### Rules every service follows

- **At least once, any order.** Every consumer is idempotent and tolerates out-of-order events (Ch. 4 › [Order tolerance and idempotence](Chapter_04_Trusting_Facts_and_Eventual_Consistency.md#order-tolerance-and-idempotence)). **Every event a service emits is complete:** a deterministic `id`, its `type`, `timestamp` and `partitionKey`, and the payload its [event fields](#event-fields) require.
- **No lost and no phantom facts.** A service never publishes a fact it hasn't recorded, and a fact it has recorded is always published in the end, even if the bus was down at the time (Ch. 5 › [Database-first event sourcing](Chapter_05_Turning_the_Cloud_into_the_Database.md#database-first-event-sourcing)).
- **Faults are events.** A poison event becomes a `fault` event on the hub instead of blocking the stream (Ch. 4 › [Fault events](Chapter_04_Trusting_Facts_and_Eventual_Consistency.md#fault-events)).
- **Nobody calls anybody.** The only synchronous call into the subsystem is the customer app to the BFF (and the card processor's webhook into the Card ESG). Not even the accounts subsystem: its data arrives as events.
- **Only ESGs cross the boundary.** The BFF and the control service consume and produce internal events only.
- **Evolve each kind of event by its own rule:**

| | Internal events | External events |
|---|---|---|
| Consumers | Services in this subsystem, all known | Other subsystems, not all known |
| Change | Freely, in small steps, following the Robustness principle ("be conservative in what you send, be liberal in what you accept") | Slowly, with **strong backward compatibility**: add fields; rename or remove only through the three deployments and a roadmap agreed with the consumers |
| May carry | `raw`, audit detail, internal codes | Only what we choose to share |
| Documented as | Lightweight schemas in our stack's own notation (the book uses TypeScript interfaces) | **JSON Schema**, published for consumers |
| Guarded by | Contract tests between our own services | Consumer-driven contract tests written by the other subsystem's team |

Sources: Ch. 2 › [Micro architecture](Chapter_02_Defining_Boundaries_and_Letting_Go.md#micro-architecture) (documenting events); Ch. 11 › [The Robustness principle](Chapter_11_Choreographing_Deployment_and_Delivery.md#the-robustness-principle), [Contract testing](Chapter_11_Choreographing_Deployment_and_Delivery.md#contract-testing).

### Simulation mode (egress ESGs)

Ours, to make the system testable without the real external systems.

- Every egress ESG that talks to an external system has one setting, **simulate**: off by default, and never on in production.
- With simulate on, the egress ESG **doesn't contact the external system**. It behaves as if it had, and then produces **the response that system would have sent, in that system's own format**, and delivers it to **the ingress ESG's real input**: the same place the real system's answer would arrive.
- **The ingress ESGs don't change** and can't tell a simulated response from a real one, so they still do their real translation and produce the real internal events.
- **The outcome depends only on the account:** success, except for the failing accounts below. **Fault accounts** still succeed, but their external system misbehaves on the way, the way it can in production (decision 18).
- **Each response comes after a delay** that can be set per external system. An account with a long delay settles after the control service's deadline, to show `PAYMENT_EXPIRED` followed by a late deposit.

| Account | Simulated outcome |
|---|---|
| `C-2` | Withdrawal rejected: insufficient funds |
| `K-3` | Card credit fails: card closed |
| `B-8` | Bank transfer rejected: no such account |
| `B-7` | Bank transfer settles, but **after** the deadline |
| `C-5` | Withdrawal succeeds, but core checking **goes down for everyone** for a minute when it arrives (fault account). Every payment in flight waits, then completes |
| `C-7` | Withdrawal succeeds, but core checking's **answer is lost**: the call times out after the debit was applied (fault account) |
| `K-6` | Card credit succeeds, but the processor **delivers the webhook twice** (fault account) |
| `B-6` | Bank transfer settles, but the network **drops the settlement file twice** (fault account) |
| any other | Success |

Each egress ESG section says what its simulated response is.

- **Simulation pairs each egress gateway with its ingress gateway.** In simulation mode, the egress gateway is configured with its partner's input: the change feed the core checking ingress reads, the card processor ingress's webhook URL **and its signing secret**, and the file location the bank network ingress watches. With simulation off, the egress gateway needs none of these.
- **Testable in simulation:** duplicate webhooks (`K-6`), duplicate files (`B-6`), "called once" when an answer is lost (`C-7`) or a request event is delivered twice, the circuit breaker during an outage (`C-5`), and pacing under the card processor's rate limit, which the simulated processor always enforces (decision 20). Closed hours are configuration and are not simulated.

---

## 1. Payment BFF

*The customer's only entry point: accepts payments and shows their progress as facts arrive.*

| | |
|---|---|
| **Pattern** | Backend for frontend |
| **Owned by** | The customer app's team |

### REST API

| Method and path | What it does | Response |
|---|---|---|
| `PUT /payments/{id}` | Submit a payment. **The client generates the id**, so a retried request creates one payment, not two. | `202 Accepted` + the payment as stored (`status: SUBMITTED`). Accepted, never "done". |
| `GET /payments/{id}` | One payment, as the customer sees it | `200` + the view below; `404` if unknown |
| `GET /payments?status=…&limit=…&cursor=…` | The customer's payments, newest first | `200` + a page of views |

**`PUT /payments/{id}` request body.** The client sends only what it decides:

```json
{
  "from": { "kind": "checking", "account": "C-1" },
  "to":   { "kind": "card", "account": "K-2" },
  "amount": "100.00",
  "currency": "USD"
}
```

- The id comes from the path. `customerId` comes from the caller's identity and `submittedAt` from the BFF's clock; if the body contains either, they're ignored. Unknown fields are ignored too.
- The `202` response body is the view (below), with `status: SUBMITTED`.

**`GET /payments` query parameters:**

| Parameter | Values | Default |
|---|---|---|
| `status` | Any `status` value from the [Status](#status) table; may be repeated | all |
| `limit` | 1–100 | 20 |
| `cursor` | The `next` value from the previous page; opaque | first page |

The page is `{ "items": [ …views… ], "next": "…" }`; `next` is absent on the last page.

**Errors** all have one shape:

```json
{ "error": "invalid-request", "message": "amount must be greater than 0", "field": "amount" }
```

| Status | `error` | When |
|---|---|---|
| `400` | `invalid-request` | Malformed body, amount ≤ 0, unknown or closed source account, bad destination (`field` names the culprit) |
| `401` | `unauthenticated` | No `X-Customer-Id` |
| `404` | `not-found` | No such payment for this customer |
| `409` | `conflict` | The same id was already used with a different body |

- Validation at the edge: amount > 0, a known **open** source account for this customer, a well-formed destination. Bad requests get `400`; nothing is written. "Known" means the BFF has heard of it through `ACCOUNT_CHANGED`: validation calls no one.
- A repeat `PUT` with the same id and the same body returns the stored payment; with a different body, `409 Conflict`.
- **The customer is taken from the caller's identity, never from the body.** For now that's the **`X-Customer-Id` header: a stand-in for an access token**, so the example can stay focused on events. It is not authentication, and it must not ship. A real token check (Ch. 10) replaces it later without changing anything else in this contract.
- A request without `X-Customer-Id` gets `401`. `GET /payments/{id}` for another customer's payment gets `404`, not `403`, so ids can't be probed.

**The view** returned by `GET` (ours):

```json
{
  "id": "p-7",
  "status": "SENT_TO_BANK",
  "amount": "100.00", "currency": "USD",
  "to": { "kind": "bank", "account": "B-9" },
  "steps": [
    { "step": "submitted",  "at": 1730900520000, "state": "done" },
    { "step": "withdrawn",  "at": 1730900580000, "state": "done" },
    { "step": "deposited",  "expected": "next business day", "state": "pending" }
  ]
}
```

### Consumed events

| Event | Shows in the view |
|---|---|
| `FUNDS_WITHDRAWN` | step *withdrawn* done |
| `WITHDRAWAL_FAILED` | the failure and its `reason` |
| `DEPOSIT_REQUESTED` | step *deposited* pending, with the expected time: "within minutes" for a card, "next business day" for a bank. The BFF works this out from `to.kind`; no event carries it |
| `FUNDS_DEPOSITED` | step *deposited* done |
| `DEPOSIT_FAILED`, `REFUND_REQUESTED`, `FUNDS_REFUNDED` | the refund and its steps |
| `PAYMENT_EXPIRED` | "delayed, we're on it" |
| `PAYMENT_COMPLETED`, `PAYMENT_FAILED` | the outcome |
| `ACCOUNT_CHANGED` | nothing in the view; it decides which source accounts the customer may use. The latest event for an account wins |

Each step shows the event's `timestamp` (when the fact happened, not when the BFF heard it). A late, repeated or out-of-order event never makes the view go backwards (Ch. 4 › [Order tolerance and idempotence](Chapter_04_Trusting_Facts_and_Eventual_Consistency.md#order-tolerance-and-idempotence)).

### Status

`status` depends **only on which events the BFF has heard, never on their order**. The first rule that matches wins:

| # | If it has heard… | `status` |
|---|---|---|
| 1 | `PAYMENT_FAILED` or `WITHDRAWAL_FAILED` | `FAILED` (with its `reason`) |
| 2 | `PAYMENT_COMPLETED` | `COMPLETED` |
| 3 | `DEPOSIT_FAILED`, `REFUND_REQUESTED` or `FUNDS_REFUNDED` | `REFUNDING` |
| 4 | `PAYMENT_EXPIRED` | `EXPIRED` ("delayed, we're on it"): **not final**; a later event still moves it on |
| 5 | `DEPOSIT_REQUESTED` | `SENT_TO_CARD` or `SENT_TO_BANK`, from `to.kind` |
| 6 | `FUNDS_WITHDRAWN` | `WITHDRAWN` |
| 7 | nothing yet | `SUBMITTED` |

- `COMPLETED` comes only from the control service's `PAYMENT_COMPLETED`. `FUNDS_DEPOSITED` alone doesn't complete the payment: the control service decides that.
- The BFF consumes `ACCOUNT_CHANGED`, never `ACCOUNTS.*`: a change in the accounts subsystem's contract stops at the accounts ingress ESG.

### Produced events

| Event | When |
|---|---|
| `PAYMENT_SUBMITTED` | A new payment is accepted: the first successful `PUT` for an id. A repeated `PUT` publishes nothing |

- **Accepted means published.** Once `PUT` has returned `202`, `PAYMENT_SUBMITTED` is published, even if the bus was down at that moment. No dual write: a payment is never accepted without its event, or published without being accepted.
- One payment, one `PAYMENT_SUBMITTED` id, however often the request is retried.

### Failure behaviour

- The bus is down: `PUT` still returns `202`; `PAYMENT_SUBMITTED` is published once the bus is back.
- The rest of the subsystem is down: payments are still accepted; the view simply stops moving.
- The accounts subsystem is down: validation uses the last known accounts. A brand-new account can't be used until its event arrives.

**Sources:** Ch. 6 › [Choosing between REST and GraphQL](Chapter_06_A_Best_Friend_for_the_Frontend.md#choosing-between-rest-and-graphql) and [REST](Chapter_06_A_Best_Friend_for_the_Frontend.md#rest); Ch. 5 › [Implementing idempotence and order tolerance](Chapter_05_Turning_the_Cloud_into_the_Database.md#implementing-idempotence-and-order-tolerance) (client-generated ids and `PUT`); Ch. 2 › [Micro architecture](Chapter_02_Defining_Boundaries_and_Letting_Go.md#micro-architecture).

---

## 2. Payment control service

*Owns the payment process: which step comes next, the undo, and the timeouts. Nothing calls it.*

| | |
|---|---|
| **Pattern** | Control service |
| **Owned by** | The payments business team |

### REST API

**None.** A control service only consumes and produces events.

### Consumed events

| Event | Means |
|---|---|
| `PAYMENT_SUBMITTED` | Start of the process |
| `FUNDS_WITHDRAWN`, `WITHDRAWAL_FAILED` | Outcome of the withdrawal |
| `FUNDS_DEPOSITED`, `DEPOSIT_FAILED` | Outcome of the deposit |
| `FUNDS_REFUNDED` | Outcome of the compensation |

- A duplicate changes nothing. Events of one payment can arrive in any order.

### Produced events (rules)

| Rule | When it has heard… | It emits |
|---|---|---|
| Start | `PAYMENT_SUBMITTED` | `WITHDRAWAL_REQUESTED` |
| Next step | `FUNDS_WITHDRAWN` | `DEPOSIT_REQUESTED` |
| Done | `FUNDS_DEPOSITED` | `PAYMENT_COMPLETED` |
| Nothing to undo | `WITHDRAWAL_FAILED` | `PAYMENT_FAILED` (reason copied) |
| Compensate | `DEPOSIT_FAILED` | `REFUND_REQUESTED` |
| Undone | `FUNDS_REFUNDED` | `PAYMENT_FAILED` |
| Timeout | `DEPOSIT_REQUESTED`, and by the deadline neither `FUNDS_DEPOSITED` nor `DEPOSIT_FAILED` | `PAYMENT_EXPIRED` |
| Late deposit | `PAYMENT_EXPIRED`, then `FUNDS_DEPOSITED` | `PAYMENT_COMPLETED` (the *Done* rule, unchanged) |
| Late failure | `PAYMENT_EXPIRED`, then `DEPOSIT_FAILED` | `REFUND_REQUESTED` (the *Compensate* rule, unchanged) |

- A rule that fires again emits the same event, with the same id, so consumers drop the repeat.
- **The deadline is configurable:** 2 days in production, short (for example 2 minutes) for demos. `PAYMENT_EXPIRED` never comes before the deadline, and may come somewhat after it.
- **A timeout is not an outcome.** `PAYMENT_EXPIRED` records that the deposit is late; it doesn't end the payment. The bank may still settle on day 3. So the rules keep running after the timeout: a late `FUNDS_DEPOSITED` still completes the payment, and a late `DEPOSIT_FAILED` still triggers the refund. Who reacts to `PAYMENT_EXPIRED` (an escalation, a customer notice) is still open (decision 9). Whoever it is will need the compensation the book warns about for expiry rules: a late event must undo an escalation that was already triggered (Ch. 8 › [expire](Chapter_08_Reacting_to_Events_with_More_Events.md#expire)).

### Failure behaviour

- A rule can't decide (an unexpected combination of events): emit a `fault` with the payment's events and stop for that payment only.

**Sources:** Ch. 8 › [Dissecting the Control Service pattern](Chapter_08_Reacting_to_Events_with_More_Events.md#dissecting-the-control-service-pattern), [expire](Chapter_08_Reacting_to_Events_with_More_Events.md#expire), [Entry and exit events](Chapter_08_Reacting_to_Events_with_More_Events.md#entry-and-exit-events), [Compensating transactions](Chapter_08_Reacting_to_Events_with_More_Events.md#compensating-transactions), [Abort events](Chapter_08_Reacting_to_Events_with_More_Events.md#abort-events).

---

## 3. Checking ESG

*Hides core checking, a legacy system: closed at night, flaky, and with no idempotency keys.*

| | |
|---|---|
| **Pattern** | ESG for a legacy system. Egress: API call behind a circuit breaker. Ingress: change data capture |
| **Name** | `payments-core-checking-egress-gateway` (egress) and `payments-core-checking-ingress-gateway` (ingress): two services |
| **Owned by** | The team that integrates with core checking |

### REST API

**None exposed.** It *calls* core checking's withdraw and refund API.

### Egress: consumed → does

| Consumes | Calls core checking | Notes |
|---|---|---|
| `WITHDRAWAL_REQUESTED` | Withdraw `amount` from `from.account`, reference `p-7` | Never publishes "it worked" (act, then hear back) |
| `REFUND_REQUESTED` | Credit `amount` back to `from.account`, reference `p-7` | Compensation, not a rollback: a new ledger entry |

- **Called once per request:** core checking has no idempotency keys, yet each `WITHDRAWAL_REQUESTED` or `REFUND_REQUESTED` reaches it once, even when the event is delivered twice. Exactly one call can't be guaranteed to a system without idempotency keys, so the ESG guarantees the next best thing: **it never calls twice blindly**. A call whose answer was lost is *in doubt*: the ESG asks core checking whether the posting exists, and calls again only if the call certainly never reached it. A call still in doubt after a grace period becomes a `fault` for an operator, and is not retried. Requests wait meanwhile; other services aren't held up.
- **Assumes an inquiry:** core checking can be asked for the postings of a reference (ours: `GET /CHK/POSTINGS?REF_NO=p-7&TXN_TYP=PMT`). Without one, a call in doubt can only become a fault.
- **Circuit breaker:** while core checking is down or closed for the night, the ESG stops calling, and requests wait until it's back, in order. A few failures in a row (timeouts, refused connections, errors; not rejections, which are answers) stop the calls; after a cool-down one request is let through as a trial, and its success lets the rest follow. Closed hours are configured, and no call is made during them. Inquiries about calls in doubt also wait, and a call in doubt gets its full grace period only once core checking is back, so an outage alone never turns waiting requests into faults. Only this ESG waits: the rest of the subsystem carries on (decision 19).
- **[Simulation mode](#simulation-mode-egress-esgs):** no call to core checking. The egress ESG writes the **ledger entry** core checking would have written (`REF_NO`, `AMT`, `STAT` = `POSTED` or `REJECTED`, `REJ_CD`) to the change feed the ingress ESG reads. Like core checking, it numbers each posting itself (`ENTRY_ID`): **a second call is a second posting**, so a double debit would show in simulation too.

### Ingress: hears → produces

| Hears (CDC on the ledger) | Produces |
|---|---|
| A debit with reference `p-7` | `FUNDS_WITHDRAWN` |
| A rejected debit with reference `p-7` (insufficient funds, account closed…) | `WITHDRAWAL_FAILED` (`reason`) |
| A credit with reference `p-7`, type refund | `FUNDS_REFUNDED` |

- The same ledger entry always yields the same event id. `raw`: the ledger row.

**Sources:** Ch. 7 › [Ingress – Change Data Capture](Chapter_07_Bridging_Intersystem_Gaps.md#ingress--change-data-capture), [Egress – circuit breaker](Chapter_07_Bridging_Intersystem_Gaps.md#egress--circuit-breaker), [Idempotence](Chapter_07_Bridging_Intersystem_Gaps.md#idempotence), [Semantic transformation](Chapter_07_Bridging_Intersystem_Gaps.md#semantic-transformation).

---

## 4. Card ESG

*Hides the card processor, a third-party SaaS: rate-limited, and it answers later through a webhook.*

| | |
|---|---|
| **Pattern** | ESG for a third-party system. Egress: API call. Ingress: webhook |
| **Name** | `payments-card-processor-egress-gateway` and `payments-card-processor-ingress-gateway`: two services |
| **Owned by** | The team that integrates with the card processor |

### REST API

| Method and path | Called by | What it does |
|---|---|---|
| `POST /webhooks/card-processor` | The card processor only (signed request) | Verifies the signature, turns the callback into our event, returns `200` fast |

### Egress: consumed → does

| Consumes | Filter | Calls the card processor |
|---|---|---|
| `DEPOSIT_REQUESTED` | `to.kind = card` | `POST /credits` with idempotency key `p-7`; expects `202 Accepted` |

- **Rate limit:** the ESG sends at the processor's allowed rate and lets the backlog wait, so it never gets `429`s. The rate is one budget for the whole ESG, however many instances read however many shards. A `429` that slips through anyway (the processor's clock or window differs from ours) is waited out and retried; that's safe because the processor dedups on the idempotency key. In simulation the processor enforces the same rate and counts the `429`s it returns (decision 20).
- **Capacity:** at peak, more card payments may arrive than the processor allows per second. Then the backlog grows during the peak and drains afterwards, and "within minutes" doesn't hold for card deposits until it has drained. The allowed rate is an agreement to size with the processor.
- A `202` is a promise, not a fact: the ESG publishes nothing.
- **[Simulation mode](#simulation-mode-egress-esgs):** no call to the card processor. After the delay, the egress ESG posts the **signed webhook** the processor would have sent (`credit.succeeded`, or `credit.failed` with `failure_code`) to the ingress ESG's `POST /webhooks/card-processor`.

### Ingress: hears → produces

| Hears (webhook) | Produces |
|---|---|
| `credit.succeeded`, ref `p-7` | `FUNDS_DEPOSITED` |
| `credit.failed`, ref `p-7` | `DEPOSIT_FAILED` (`reason`) |

- Event id: derived from the webhook's own event id, so a redelivered webhook is the same event. `raw`: the webhook body.

**Sources:** Ch. 7 › [Egress – API call](Chapter_07_Bridging_Intersystem_Gaps.md#egress--api-call), [Ingress – webhook](Chapter_07_Bridging_Intersystem_Gaps.md#ingress--webhook), [Idempotence](Chapter_07_Bridging_Intersystem_Gaps.md#idempotence).

---

## 5. Bank ESG

*Hides the other banks, a batch network: files go out at night, settlement files come back the next day.* **Our framing:** the book doesn't describe a file-based ESG.

| | |
|---|---|
| **Pattern** | ESG for a batch system: events become files, files become events |
| **Name** | `payments-bank-network-egress-gateway` and `payments-bank-network-ingress-gateway`: two services |
| **Owned by** | The team that integrates with the bank network |

### REST API

**None.** Files are exchanged through the bank network's file transfer.

### Egress: consumed → does

| Consumes | Filter | Does |
|---|---|---|
| `DEPOSIT_REQUESTED` | `to.kind = bank` | Adds the payment to tonight's transfer file (one line per payment, keyed by `p-7`) |

- A payment appears in a transfer file once, even if `DEPOSIT_REQUESTED` arrives twice.
- The file goes out each night at the bank network's cut-off.
- **[Simulation mode](#simulation-mode-egress-esgs):** the transfer file isn't sent. After the delay, the egress ESG drops the **settlement file** the network would have returned (`reference,status,code`) where the ingress ESG picks up files.

### Ingress: hears → produces

| Hears (settlement file, one line per payment) | Produces |
|---|---|
| Settled, ref `p-7` | `FUNDS_DEPOSITED` |
| Rejected, ref `p-7` | `DEPOSIT_FAILED` (`reason`) |

- Event id: file name + line reference, so reprocessing the same file produces the same events. `raw`: the line.

**Sources:** Ch. 7 › [Dissecting the External Service Gateway pattern](Chapter_07_Bridging_Intersystem_Gaps.md#dissecting-the-external-service-gateway-pattern) (ingress and egress flows), [Idempotence](Chapter_07_Bridging_Intersystem_Gaps.md#idempotence).

---

## 6. Event hub

*The subsystem's backbone: producers publish to one bus; routing rules decide where events go.*

| | |
|---|---|
| **Pattern** | Event hub: bus, channels, event lake |
| **Owned by** | The subsystem's platform team |

### Interface

- **Publish:** any service puts envelope-shaped events on the bus. No REST API.
- **Consume:** a service reads its channel (a stream), at its own pace, by `partitionKey`.

### Routing rules

| Rule | Matches | Goes to |
|---|---|---|
| Event lake | Every event, internal and external, **except `fault`** | The archive, kept forever. Faults are kept by the fault monitor instead |
| Default channel | Internal events (`source` ≠ `external`), all but `fault` | The default stream |
| Faults | `type = fault` | The fault monitor's channel |
| **Egress** *(in `payments-egress-gateway`'s stack)* | `source = external` **and** `type` starts with `PAYMENTS.` | The downstream subsystem's bus. One target per downstream subsystem |
| **Ingress channel** | `source = external` **and** `type` starts with `ACCOUNTS.` | The ingress stream, read only by the accounts ingress ESG |
| A consumer's own channel | A consumer's own rule, when it needs isolation or its own throughput | Its own stream |

- **Why the egress rule needs the prefix.** The book's rule matches `source: external` alone. Our bus carries external events in **both** directions: the `PAYMENTS.*` we publish and the `ACCOUNTS.*` that arrive. Matching on `source` alone would forward the accounts subsystem's events on to the downstream subsystem.
- **One cloud account for the whole system** (see [Decisions](#decisions)): each subsystem has its own bus, but they share an account, so the book's cross-account trust agreement isn't needed between subsystems. The external systems (core checking, card processor, bank network) live in **other** accounts, and only their ESGs reach across.
- **The egress rule lives with the egress gateway**, not in the hub: everything about the external contract (the mapping and where it goes) is in one service, and the hub depends on no neighbour.
- **The ingress channel is required here.** The book calls it optional (Ch. 7 › [Ingress – downstream subsystem](Chapter_07_Bridging_Intersystem_Gaps.md#ingress--downstream-subsystem)), but our default channel excludes `source = external`, as the existing example does, so without an ingress channel the `ACCOUNTS.*` events would reach no stream.

### Capacity

- Peak: about 10,000 events/s. Size the streams for the bus's rate so no consumer is throttled.
- Channels keep events for days: a consumer that's down catches up when it comes back.
- **Replay:** from the lake, to one service only, never to the whole bus.

**Fault monitor** (mentioned only, outside the talk): a separate service reads the faults channel, alerts, and lets an operator resubmit the affected events once the cause is fixed. The hub's routing is the dead-letter queue; nothing special is built. Sources: Ch. 4 › [Fault events](Chapter_04_Trusting_Facts_and_Eventual_Consistency.md#fault-events); Ch. 12 › [Errors and fault events](Chapter_12_Optimizing_Observability.md#errors-and-fault-events).

**Sources:** Ch. 2 › [Event hub](Chapter_02_Defining_Boundaries_and_Letting_Go.md#event-hub); Ch. 4 › [Publishing to an event hub](Chapter_04_Trusting_Facts_and_Eventual_Consistency.md#publishing-to-an-event-hub) and [Event lake](Chapter_04_Trusting_Facts_and_Eventual_Consistency.md#event-lake); Ch. 7 › [Integrating with other subsystems](Chapter_07_Bridging_Intersystem_Gaps.md#integrating-with-other-subsystems).

---

## 7. Payments egress ESG

*Decides what the payment subsystem tells the rest of the bank, and hides everything else.*

| | |
|---|---|
| **Pattern** | ESG for an upstream subsystem's egress: internal events in, external events out |
| **Name** | `payments-egress-gateway`, after the book's `<system>-ingress\|egress-<capability>-gateway` convention |
| **Also owns** | The **egress rule** on our bus: `source = external` and a `PAYMENTS.` type → the downstream subsystem's bus |
| **Owned by** | The payments business team, because it owns the contract |

### REST API

**None.**

### Consumed → produced

| Consumes (internal) | Produces (external) | Mapping |
|---|---|---|
| `PAYMENT_COMPLETED` | `PAYMENTS.PAYMENT_COMPLETED` | |
| `PAYMENT_FAILED` | `PAYMENTS.PAYMENT_FAILED` | `reason` mapped to the public list |

**The external payload** (ours), the same for both:

```json
{
  "id": "…",
  "type": "PAYMENTS.PAYMENT_FAILED",
  "timestamp": 1731073320000,
  "partitionKey": "p-7",
  "tags": { "source": "payments-egress-gateway", "subsystem": "payments" },
  "payment": {
    "id": "p-7",
    "customerId": "u-42",
    "amount": "100.00",
    "currency": "USD",
    "destination": "bank",
    "submittedAt": 1730900520000
  },
  "reason": "account-closed"
}
```

- **Shared:** payment id, `customerId` (so a downstream subsystem can contact the customer), amount, currency, destination kind (`card` or `bank`), submitted time, and, on `PAYMENTS.PAYMENT_FAILED` only, the event's `reason`.
- **Not shared:** `raw`, the source and destination account numbers, internal reason codes, and every step event (`WITHDRAWAL_REQUESTED`, `FUNDS_WITHDRAWN`, …). Neighbours see outcomes, not our process.
- **Public `reason` list:** `insufficient-funds`, `account-closed`, `account-not-found`, `rejected`. A new internal reason maps to `rejected` until we decide to publish it.
- Event id: derived from the internal event's id, so a redelivered internal event produces the same external event.
- `customerId` is personal data crossing a boundary. If it ever needs protecting, the envelope's `eem` field is the book's mechanism (Ch. 10).
- Published as a **JSON Schema** for consumers. A change follows the external rule in [Rules every service follows](#rules-every-service-follows).

**Sources:** Ch. 7 › [Egress – upstream subsystem](Chapter_07_Bridging_Intersystem_Gaps.md#egress--upstream-subsystem), [Packaging](Chapter_07_Bridging_Intersystem_Gaps.md#packaging), [Semantic transformation](Chapter_07_Bridging_Intersystem_Gaps.md#semantic-transformation); Ch. 2 › [External domain events](Chapter_02_Defining_Boundaries_and_Letting_Go.md#external-domain-events).

---

## 8. Accounts ingress ESG

*The anti-corruption layer between the accounts subsystem's contract and ours.*

| | |
|---|---|
| **Pattern** | ESG for a downstream subsystem's ingress: external events in, internal events out |
| **Name** | `payments-accounts-ingress-gateway` |
| **Owned by** | The customer app's team, because the BFF is the only consumer |

### REST API

**None.**

### Consumed → produced

| Consumes (external, the accounts subsystem's contract) | Produces (internal) |
|---|---|
| `ACCOUNTS.ACCOUNT_OPENED` | `ACCOUNT_CHANGED` |
| `ACCOUNTS.ACCOUNT_UPDATED` | `ACCOUNT_CHANGED` |
| `ACCOUNTS.ACCOUNT_CLOSED` | `ACCOUNT_CHANGED` (`status: closed`) |

The incoming payload, as the accounts subsystem defines it:

```json
{ "type": "ACCOUNTS.ACCOUNT_OPENED", "partitionKey": "C-1",
  "account": { "number": "C-1", "kind": "checking", "holderId": "u-42", "status": "open", "currency": "USD" } }
```

The internal event (ours), carrying only what the BFF needs:

```json
{ "type": "ACCOUNT_CHANGED", "partitionKey": "C-1",
  "tags": { "source": "payments-accounts-ingress-gateway" },
  "account": { "number": "C-1", "kind": "checking", "customerId": "u-42", "status": "open" },
  "raw": { "…": "the accounts event, unchanged" } }
```

- `holderId` becomes `customerId`, our name. When the accounts team renames `holderId` to `ownerId`, this ESG reads either field during their three deployments; the BFF never notices.
- Event id: derived from the external event's id. `raw`: the external event, so the lake keeps exactly what we heard.
- The ESG ignores fields it doesn't know (liberal in what it accepts).

**Sources:** Ch. 7 › [Ingress – downstream subsystem](Chapter_07_Bridging_Intersystem_Gaps.md#ingress--downstream-subsystem); Ch. 11 › [The Robustness principle](Chapter_11_Choreographing_Deployment_and_Delivery.md#the-robustness-principle).

---

## Decisions

| # | Question | Decision (2026-11-06) |
|---|---|---|
| 1 | `PUT` or `POST` for submitting a payment? | **`PUT /payments/{id}`** with a client-generated id, everywhere in the deck and diagrams (the book's recommendation, so a retry is still one payment) |
| 2 | A deposit after the timeout? | `PAYMENT_EXPIRED` is **not an outcome**: the rules keep running, and a late deposit or failure still completes or refunds the payment (see the [control service](#2-payment-control-service)) |
| 3 | The fault monitor? | **One-line mention** under the [event hub](#6-event-hub); outside the talk |
| 4 | Internal and external events? (2026-11-07) | **Split them**, as the book does. Every catalog event is internal; other subsystems get `PAYMENTS.PAYMENT_COMPLETED` and `PAYMENTS.PAYMENT_FAILED` from the [payments egress ESG](#7-payments-egress-esg), and the BFF gets accounts through the [accounts ingress ESG](#8-accounts-ingress-esg) as `ACCOUNT_CHANGED` |
| 5 | Include `customerId` in external events? (2026-11-07) | **Yes**, so a downstream subsystem can contact the customer. Account numbers stay inside |
| 6 | Tech stack? (2026-11-07) | **Not the book's TypeScript framework**; framework and language to be chosen. The specs avoid tech assumptions: no products are named, and every service is a black box (decision 15) |
| 7 | Cloud accounts? (2026-11-07) | **The whole system in one account.** Payments, accounts and the downstream subsystem each get their own event bus, but not their own account. **The external systems are in different accounts.** This departs from the book, which fortifies each autonomous subsystem in its own cloud account (Ch. 2 › [Creating subsystem bulkheads](Chapter_02_Defining_Boundaries_and_Letting_Go.md#creating-subsystem-bulkheads)); the separate buses keep the internal/external split visible |
| 8 | What does the event lake keep? (2026-11-07) | **Every event except `fault`**, the same as the existing example the app is modelled on. Faults go to the fault monitor |
| 9 | What happens with `PAYMENT_EXPIRED`? (2026-11-07) | **It's an internal event only**, consumed by the BFF (status `EXPIRED`). It isn't published to other subsystems. Whether it starts an escalation, notifies the customer, or something else is decided later |
| 10 | Event model? (2026-11-07) | One shared `payment` entity plus a few fields per event ([Event fields](#event-fields)). **`reason` is on the event, not the payment.** **Amounts are decimal strings** (`"100.00"`). The BFF works out the expected deposit time from `to.kind`; no event carries it |
| 11 | How are event types spelled? (2026-11-07) | **Upper snake case**, like the existing example: `PAYMENT_SUBMITTED`. External events add a namespace prefix with a dot: `PAYMENTS.PAYMENT_COMPLETED`, `ACCOUNTS.ACCOUNT_OPENED`. `fault` keeps the framework's name. Reason values stay lower kebab case (`insufficient-funds`): they're data, not types |
| 12 | How does the BFF view stay correct with events out of order? (2026-11-07) | **`status` depends only on which events the BFF has heard**, by a first-match rule table, never on their order |
| 13 | Where does `customerId` come from? (2026-11-07) | **The `X-Customer-Id` header for now**, explicitly a stand-in for an access token, not authentication. A real token check replaces it later |
| 14 | Service names and deploy order? (2026-11-07) | Prefix **`payments`**; **separate ingress and egress gateways** for every external system; the **egress rule moves to `payments-egress-gateway`**. See [Services and deploy order](#services-and-deploy-order). The ingress channel becomes required, because the default channel excludes external events |
| 15 | How much of each service does the spec describe? (2026-11-07) | **Only the outside: every service is a black box.** Inputs, outputs, API, observable behaviour and guarantees; nothing about functions, tables, keys, timers or how idempotence is achieved |
| 16 | How do we test without the external systems? (2026-11-07) | **A simulation mode in each egress ESG**, off by default. It pretends to call the external system and delivers that system's response, in its own format, to the **ingress ESG's real input**, so the ingress side is exercised unchanged. Outcomes by account (success or failure only, for now). This replaces the separate stub services |
| 17 | Consolidation (2026-11-07) | `PUT` body, paging, `status` filter and error format defined; `publish-accounts` runs before the first payment; simulation pairs egress with ingress (including the webhook secret); guarantees that simulation can't show yet are marked; the talk's "Built in: Stage" rows removed; the account-outcome table lives here only |
| 18 | How are faults simulated? (2026-11-09) | **Fault accounts**, in the same style as the outcome accounts: the account decides how the external system misbehaves, with no runtime switches. First: `K-6` (webhook delivered twice) and `B-6` (settlement file dropped twice). The core checking simulation numbers every posting, so calling it twice posts twice. Then (2026-11-09): `C-7`, an answer lost after the debit ("called once"), and `C-5`, an outage (circuit breaker, decision 19). Then: the card processor's rate limit, which the simulated processor always enforces (decision 20) |
| 19 | How do requests wait while core checking is down? (2026-11-09) | **In the ESG itself:** a breaker shared by its instances; while it is open the ESG doesn't call, and gives the requests back to its channel, which keeps them in order, at a paced rate. Production refinement, not built: switch the ESG's subscription off while the breaker is open, and back on from a scheduled health check, so nothing runs during a long outage or the night (needs a health-check API on core checking). Outages are simulated with `C-5`, the first fault felt by every payment |
| 20 | How does the card ESG keep to the processor's rate? (2026-11-09) | **A shared per-second budget:** every call takes a token first, from a budget shared by all instances of the ESG, and waits for the next second when there is none, so the backlog waits on the stream. `429` is only a safety net (retry after `Retry-After`; safe thanks to the idempotency key). Not built: leasing tokens in blocks for high rates. **Simulation:** the simulated processor always enforces the same rate and counts its `429`s; a rate limit is a property of the processor, so there is no fault account for it |
