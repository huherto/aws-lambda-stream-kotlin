package org.myorg.payments

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.utils.loggedLazy
import mu.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/** The card processor's API. A `202` is a promise, not a fact: the outcome arrives later through the webhook. */
fun interface CardProcessor {
    suspend fun credit(payment: Payment)
}

/**
 * The real API: `POST /v1/credits` with the payment id as idempotency key. Not deployed in this example.
 * Still to build: pacing under the processor's rate limit, so the backlog waits instead of getting `429`s.
 */
class HttpCardProcessor(
    private val baseUrl: String,
    private val apiKey: String,
    private val http: HttpClient = HttpClient.newHttpClient(),
) : CardProcessor {
    override suspend fun credit(payment: Payment) {
        val body = """{"card":"${payment.to.account}","amount_cents":${amountInCents(payment.amount)},""" +
            """"currency":"${payment.currency.lowercase()}","metadata":{"reference":"${payment.id}"}}"""
        val response = http.send(
            HttpRequest.newBuilder(URI.create("$baseUrl/v1/credits"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer $apiKey")
                .header("Idempotency-Key", payment.id)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        check(response.statusCode() == 202) { "card processor answered ${response.statusCode()}" }
    }
}

/** Simulation mode: no call. Schedules the webhook the processor would have sent, a few seconds later. */
class SimulatedCardProcessor(
    private val delivery: DelayedDelivery,
    private val delaySeconds: Int,
    private val clock: () -> Long = System::currentTimeMillis,
) : CardProcessor {
    override suspend fun credit(payment: Payment) {
        val webhook = simulatedCardWebhook(payment, createdSeconds = clock() / 1000 + delaySeconds)
        val body = cardJson.encodeToString(CardWebhook.serializer(), webhook)
        // K-6: the processor delivers the same webhook (same event_id) more than once.
        repeat(Simulation.deliveries(payment.to.account)) { n ->
            delivery.deliverLater(body, delaySeconds + n * Simulation.REDELIVERY_SECONDS)
        }
    }
}

class EgressContainer(val cardProcessor: CardProcessor) {
    companion object {
        fun build(): EgressContainer = EgressContainer(
            if (Simulation.enabled()) SimulatedCardProcessor(SqsDelayedDelivery.fromEnvironment(), Simulation.delaySeconds(5))
            else HttpCardProcessor(
                System.getenv("CARD_PROCESSOR_URL") ?: error("CARD_PROCESSOR_URL is not configured"),
                System.getenv("CARD_PROCESSOR_API_KEY") ?: error("CARD_PROCESSOR_API_KEY is not configured"),
            ),
        )
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(
            ActionPipeline.builder()
                .id("credit")
                .eventFilter(depositsTo(Party.CARD))
                .action { uow -> cardProcessor.credit((uow.event as PaymentEvent).payment) }
                .build()
        )
        .build()

    val kinesisAdapter = KinesisAdapter(eventCodec = PaymentsEventCodec)
}

class Listener(
    containerFactory: () -> EgressContainer = { EgressContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {
    private val container by loggedLazy("EgressContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        runKinesis(container.assembler, container.kinesisAdapter, event)
        return null
    }
}

/**
 * Simulation only: signs the delayed webhook with the ingress gateway's secret and posts it to its real URL.
 * A non-2xx answer fails the message, so the queue redelivers it, as the processor would.
 */
class Simulator(
    private val webhookUrl: String = System.getenv("WEBHOOK_URL") ?: "undefined",
    private val secret: String = System.getenv("WEBHOOK_SECRET") ?: "undefined",
    private val http: HttpClient = HttpClient.newHttpClient(),
) : RequestHandler<SQSEvent, Void?> {
    private val logger = KotlinLogging.logger { }

    override fun handleRequest(event: SQSEvent, context: Context): Void? {
        for (message in event.records) {
            val body = message.body
            val signature = CardSignature.sign(secret, System.currentTimeMillis() / 1000, body)
            val response = http.send(
                HttpRequest.newBuilder(URI.create(webhookUrl))
                    .header("Content-Type", "application/json")
                    .header(CardSignature.HEADER, signature)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
            logger.info { "webhook answered ${response.statusCode()}: ${response.body()}" }
            check(response.statusCode() in 200..299) { "webhook answered ${response.statusCode()}" }
        }
        return null
    }
}
