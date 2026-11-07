package org.myorg.payments

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.JsonRaw
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import mu.KotlinLogging

/**
 * | Hears (webhook)              | Produces                    |
 * |------------------------------|-----------------------------|
 * | `credit.succeeded`, ref p-7  | FUNDS_DEPOSITED             |
 * | `credit.failed`, ref p-7     | DEPOSIT_FAILED (`reason`)   |
 *
 * The event id derives from the webhook's own event id, so a redelivered webhook is the same event.
 * `raw` is the webhook body. Returns `null` for webhook types we don't handle.
 */
fun webhookEvent(webhook: CardWebhook, body: String, payment: Payment): PaymentEvent? {
    val raw = JsonRaw(Json.parseToJsonElement(body))
    val timestamp = webhook.created * 1000
    return when (webhook.type) {
        CardWebhook.SUCCEEDED ->
            FundsDeposited(payment, "${webhook.eventId}.funds-deposited", timestamp, payment.id, raw = raw)
        CardWebhook.FAILED ->
            DepositFailed(payment, cardProcessorReason(webhook.data.failureCode), "${webhook.eventId}.deposit-failed",
                timestamp, payment.id, raw = raw)
        else -> null
    }
}

class WebhookContainer(
    val secret: String,
    val lookup: PaymentLookup,
    val eventPublisher: EventPublisher,
    val clock: () -> Long = System::currentTimeMillis,
) {
    companion object {
        fun build(): WebhookContainer {
            installGatewayFaultManager()
            return WebhookContainer(
                secret = System.getenv("WEBHOOK_SECRET") ?: error("WEBHOOK_SECRET is not configured"),
                lookup = DynamoDbPaymentLookup.fromEnvironment(),
                eventPublisher = AwsGlobalRegistry.eventPublisher(),
            )
        }
    }
}

/**
 * `POST /webhooks/card-processor`, called by the card processor only. Verifies the signature, turns the callback
 * into our event and returns `200` once it is published. Anything else makes the processor redeliver, which is safe:
 * a redelivered webhook is the same event.
 */
class Webhook(
    containerFactory: () -> WebhookContainer = { WebhookContainer.build() },
) : RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private val logger = KotlinLogging.logger { }
    private val container by loggedLazy("WebhookContainer", logger, containerFactory)

    override fun handleRequest(input: APIGatewayProxyRequestEvent, context: Context): APIGatewayProxyResponseEvent =
        runBlocking {
            val body = input.body ?: return@runBlocking respond(400, "missing body")
            val signature = input.headers?.entries?.firstOrNull { it.key.equals(CardSignature.HEADER, true) }?.value
            if (!CardSignature.verify(container.secret, signature, body, container.clock() / 1000)) {
                return@runBlocking respond(401, "invalid signature")
            }

            val webhook = try {
                cardJson.decodeFromString(CardWebhook.serializer(), body)
            } catch (e: Exception) {
                logger.warn(e) { "Unreadable webhook" }
                return@runBlocking respond(400, "unreadable webhook")
            }

            try {
                val payment = container.lookup.payment(webhook.data.metadata.reference)
                val event = webhookEvent(webhook, body, payment) ?: return@runBlocking respond(200, "ignored")
                val published = container.eventPublisher.publish(flowOf(UnitOfWork(event = event))).toList()
                logger.info { "published ${event.eventType()} ${event.id} (${published.size})" }
                respond(200, "ok")
            } catch (e: PaymentNotKnownYet) {
                logger.warn { e.message }
                respond(503, "not ready, retry later")
            } catch (e: Exception) {
                logger.error(e) { "Could not publish webhook ${webhook.eventId}" }
                respond(500, "could not process webhook")
            }
        }

    private fun respond(status: Int, message: String) = APIGatewayProxyResponseEvent()
        .withStatusCode(status)
        .withHeaders(mapOf("Content-Type" to "application/json"))
        .withBody("""{"message":"$message"}""")
}

/** Default channel → the card deposits we asked the processor for. */
class RequestsContainer(table: String) {
    companion object {
        fun build() = RequestsContainer(System.getenv("REQUESTS_TABLE_NAME") ?: error("REQUESTS_TABLE_NAME is not configured"))
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(
            requestsPipeline(
                "requests", table,
                depositsTo(Party.CARD),
            )
        )
        .build()

    val kinesisAdapter = KinesisAdapter(eventCodec = PaymentsEventCodec)
}

class Listener(
    containerFactory: () -> RequestsContainer = { RequestsContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {
    private val container by loggedLazy("RequestsContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        runKinesis(container.assembler, container.kinesisAdapter, event)
        return null
    }
}
