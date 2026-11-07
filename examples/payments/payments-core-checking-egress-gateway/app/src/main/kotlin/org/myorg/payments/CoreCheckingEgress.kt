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
}

/**
 * The real API: `POST /CHK/DEBIT` and `POST /CHK/CREDIT`. Not deployed in this example (core checking lives in
 * another account). Still to build: the circuit breaker for the nights it is closed, and "called once" for a
 * system without idempotency keys.
 */
class HttpCoreChecking(private val baseUrl: String, private val http: HttpClient = HttpClient.newHttpClient()) : CoreChecking {
    override suspend fun debit(payment: Payment) = post("/CHK/DEBIT", payment)
    override suspend fun credit(payment: Payment) = post("/CHK/CREDIT", payment)

    private fun post(path: String, payment: Payment) {
        val body = """{"REF_NO":"${payment.id}","ACCT_NO":"${payment.from.account}","AMT":"${payment.amount}"}"""
        val response = http.send(
            HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        check(response.statusCode() in 200..299) { "core checking $path answered ${response.statusCode()}" }
    }
}

/** Simulation mode: no call. Schedules the ledger row core checking would have written. */
class SimulatedCoreChecking(
    private val delivery: DelayedDelivery,
    private val delaySeconds: Int,
    private val clock: () -> Long = System::currentTimeMillis,
) : CoreChecking {
    override suspend fun debit(payment: Payment) = schedule(payment, LedgerEntry.PAYMENT)
    override suspend fun credit(payment: Payment) = schedule(payment, LedgerEntry.REFUND)

    private suspend fun schedule(payment: Payment, txnType: String) {
        val entry = simulatedLedgerEntry(payment, txnType, postedAt = clock() + delaySeconds * 1000L)
        delivery.deliverLater(Json.encodeToString(LedgerEntry.serializer(), entry), delaySeconds)
    }
}

class EgressContainer(val coreChecking: CoreChecking) {
    companion object {
        fun build(): EgressContainer = EgressContainer(
            if (Simulation.enabled()) SimulatedCoreChecking(SqsDelayedDelivery.fromEnvironment(), Simulation.delaySeconds(2))
            else HttpCoreChecking(System.getenv("CORE_CHECKING_URL") ?: error("CORE_CHECKING_URL is not configured")),
        )
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(
            ActionPipeline.builder()
                .id("withdraw")
                .eventFilter(EventFilters.name(PaymentsEvent.WITHDRAWAL_REQUESTED))
                .action { uow -> coreChecking.debit((uow.event as PaymentEvent).payment) }
                .build()
        )
        .addPipeline(
            ActionPipeline.builder()
                .id("refund")
                .eventFilter(EventFilters.name(PaymentsEvent.REFUND_REQUESTED))
                .action { uow -> coreChecking.credit((uow.event as PaymentEvent).payment) }
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
