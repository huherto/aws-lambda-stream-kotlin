package org.myorg.payments

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * Core checking's withdraw and refund API. It never says "it worked": the answer comes from its ledger,
 * through the ingress gateway's change data capture ("act, then hear back").
 */
interface CoreChecking {
    suspend fun debit(payment: Payment)
    suspend fun credit(payment: Payment)

    /** Whether core checking shows a posting (posted or rejected) for this payment and transaction type. */
    suspend fun posted(payment: Payment, txnType: String): Boolean
}

/**
 * The real API: `POST /CHK/DEBIT` and `POST /CHK/CREDIT`. Not deployed in this example (core checking lives in
 * another account). Still to build: the circuit breaker for the nights it is closed, and "called once" for a
 * system without idempotency keys.
 */
class HttpCoreChecking(private val baseUrl: String, private val http: HttpClient = HttpClient.newHttpClient()) : CoreChecking {
    override suspend fun debit(payment: Payment) = post("/CHK/DEBIT", payment)
    override suspend fun credit(payment: Payment) = post("/CHK/CREDIT", payment)

    /** Assumes an inquiry by reference, `GET /CHK/POSTINGS?REF_NO=…&TXN_TYP=…`, answering 200 or 404. */
    override suspend fun posted(payment: Payment, txnType: String): Boolean {
        val response = http.send(
            HttpRequest.newBuilder(URI.create("$baseUrl/CHK/POSTINGS?REF_NO=${payment.id}&TXN_TYP=$txnType")).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        return when (response.statusCode()) {
            200 -> true
            404 -> false
            else -> error("core checking inquiry answered ${response.statusCode()}")
        }
    }

    private fun post(path: String, payment: Payment) {
        val body = """{"REF_NO":"${payment.id}","ACCT_NO":"${payment.from.account}","AMT":"${payment.amount}"}"""
        val response = try {
            http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        } catch (e: java.net.ConnectException) {
            throw CallNotSent("core checking refused the connection", e)
        }
        check(response.statusCode() in 200..299) { "core checking $path answered ${response.statusCode()}" }
    }
}

/**
 * Simulation mode: no call. Schedules the ledger row core checking would have written. In simulation core checking's
 * state *is* the ledger, so the inquiry reads it.
 */
class SimulatedCoreChecking(
    private val delivery: DelayedDelivery,
    private val delaySeconds: Int,
    private val postings: suspend (reference: String, txnType: String) -> Boolean = { _, _ -> false },
    private val outage: SimulatedOutage = SimulatedOutage.none(),
    private val lost: SimulatedOnce = SimulatedOnce.never(),
    private val clock: () -> Long = System::currentTimeMillis,
    /** Core checking numbers every posting itself; a second call is a second posting. */
    private val newEntryId: () -> String = { "E" + java.util.UUID.randomUUID().toString().replace("-", "").take(16).uppercase() },
) : CoreChecking {
    /** C-5 takes core checking down; while it's down, nothing reaches it: the connection is refused. */
    private suspend fun reachable(payment: Payment, starting: Boolean = false) {
        if (starting && payment.from.account == Simulation.OUTAGE_ACCOUNT) outage.startOnce(payment.id, clock())
        if (outage.isDown(clock())) throw CallNotSent("simulated: core checking is down (connection refused)")
    }

    override suspend fun debit(payment: Payment) {
        reachable(payment, starting = true)
        // C-8: the first call is lost on the way; core checking never sees it.
        if (payment.from.account == Simulation.LOST_REQUEST_ACCOUNT && lost.firstTime("lost#${payment.id}")) {
            throw java.net.SocketTimeoutException("simulated: the call timed out and core checking never got it")
        }
        schedule(payment, LedgerEntry.PAYMENT)
        // C-7: the debit is applied, but the answer never comes back.
        if (payment.from.account == Simulation.TIMEOUT_AFTER_DEBIT_ACCOUNT) {
            throw java.net.SocketTimeoutException("simulated: core checking applied the debit but the call timed out")
        }
    }

    override suspend fun credit(payment: Payment) {
        reachable(payment)
        schedule(payment, LedgerEntry.REFUND)
    }

    override suspend fun posted(payment: Payment, txnType: String): Boolean {
        reachable(payment)
        return postings(payment.id, txnType)
    }

    private suspend fun schedule(payment: Payment, txnType: String) {
        val entry = simulatedLedgerEntry(payment, txnType, postedAt = clock() + delaySeconds * 1000L, entryId = newEntryId())
        delivery.deliverLater(Json.encodeToString(LedgerEntry.serializer(), entry), delaySeconds)
    }
}

class EgressContainer(val calledOnce: CalledOnce) {
    companion object {
        fun build(): EgressContainer {
            installCoreCheckingFaultManager()
            val dynamoDb = AwsGlobalRegistry.dynamoDbClientFactory().getClient("calls")
            val coreChecking =
                if (Simulation.enabled()) SimulatedCoreChecking(
                    SqsDelayedDelivery.fromEnvironment(),
                    Simulation.delaySeconds(2),
                    postings = LedgerPostings(dynamoDb, System.getenv("LEDGER_TABLE_NAME") ?: error("LEDGER_TABLE_NAME is not configured")),
                    lost = DynamoDbSimulatedOnce(dynamoDb, System.getenv("CALLS_TABLE_NAME")!!),
                    outage = DynamoDbSimulatedOutage(
                        dynamoDb, System.getenv("CALLS_TABLE_NAME")!!,
                        lasts = kotlin.time.Duration.parse("${System.getenv("SIMULATED_OUTAGE_SECONDS")?.toLongOrNull() ?: 60}s"),
                    ),
                )
                else HttpCoreChecking(System.getenv("CORE_CHECKING_URL") ?: error("CORE_CHECKING_URL is not configured"))
            val claims = DynamoDbClaims(dynamoDb, System.getenv("CALLS_TABLE_NAME") ?: error("CALLS_TABLE_NAME is not configured"))
            val grace = System.getenv("CALL_GRACE_SECONDS")?.toLongOrNull() ?: 300
            val breaker = Breaker(
                store = DynamoDbBreakerStore(dynamoDb, System.getenv("CALLS_TABLE_NAME")!!),
                threshold = System.getenv("BREAKER_THRESHOLD")?.toIntOrNull() ?: 3,
                openFor = kotlin.time.Duration.parse("${System.getenv("BREAKER_OPEN_SECONDS")?.toLongOrNull() ?: 30}s"),
                closedHours = ClosedHours.parse(System.getenv("CORE_CHECKING_CLOSED_HOURS")),
            )
            return EgressContainer(CalledOnce(coreChecking, claims, grace = kotlin.time.Duration.parse("${grace}s"), breaker = breaker))
        }
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(
            ActionPipeline.builder()
                .id("withdraw")
                .eventFilter(EventFilters.name(PaymentsEvent.WITHDRAWAL_REQUESTED))
                .action { uow -> calledOnce.debit((uow.event as PaymentEvent).payment, request = uow.event.toString()) }
                .build()
        )
        .addPipeline(
            ActionPipeline.builder()
                .id("refund")
                .eventFilter(EventFilters.name(PaymentsEvent.REFUND_REQUESTED))
                .action { uow -> calledOnce.credit((uow.event as PaymentEvent).payment, request = uow.event.toString()) }
                .build()
        )
        .build()

    val kinesisAdapter = KinesisAdapter(eventCodec = PaymentsEventCodec)
}

/** Default channel → core checking. */
class Listener(
    containerFactory: () -> EgressContainer = { EgressContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {
    private val container by loggedLazy("EgressContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        runKinesis(container.assembler, container.kinesisAdapter, event)
        return null
    }
}

/** Simulation only: writes the delayed ledger row to the change feed the ingress gateway reads. */
class Simulator : RequestHandler<SQSEvent, Void?> {
    private val logger = KotlinLogging.logger { }
    private val ledgerTable = System.getenv("LEDGER_TABLE_NAME") ?: "undefined"

    override fun handleRequest(event: SQSEvent, context: Context): Void? = runBlocking {
        val client = AwsGlobalRegistry.dynamoDbClientFactory().getClient("simulator")
        for (message in event.records) {
            val entry = Json.decodeFromString(LedgerEntry.serializer(), message.body)
            client.putItem(PutItemRequest {
                tableName = ledgerTable
                item = entry.toAttributes().mapValues { AttributeValue.S(it.value) }
            })
            logger.info { "simulated ledger row ${entry.ENTRY_ID} ${entry.STAT}" }
        }
        null
    }
}

/** In simulation, core checking's postings are the ledger rows, found by reference through the ledger's index. */
class LedgerPostings(
    private val client: aws.sdk.kotlin.services.dynamodb.DynamoDbClient,
    private val table: String,
) : suspend (String, String) -> Boolean {
    override suspend fun invoke(reference: String, txnType: String): Boolean =
        client.query(aws.sdk.kotlin.services.dynamodb.model.QueryRequest {
            tableName = table
            indexName = "byReference"
            keyConditionExpression = "REF_NO = :r AND TXN_TYP = :t"
            expressionAttributeValues = mapOf(":r" to AttributeValue.S(reference), ":t" to AttributeValue.S(txnType))
            limit = 1
        }).count > 0
}

/**
 * While core checking is down or refuses the connection, requests are retried by the stream
 * (with `STREAM_RETRY_ENABLED=true`) instead of becoming faults. A call unresolved past the grace period does become
 * a fault, for an operator.
 */
fun installCoreCheckingFaultManager() {
    AwsGlobalRegistry.setFaultManagerFactory {
        io.kopipes.core.faults.FaultManager(
            eventPublisher = AwsGlobalRegistry.eventPublisher(),
            isRetryable = {
                it is CallNotSent || it is CoreCheckingUnavailable || AwsGlobalRegistry.isAwsRetryable(it)
            },
        )
    }
}
