package org.myorg.payments

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.DeleteMessageRequest
import aws.sdk.kotlin.services.sqs.model.GetQueueUrlRequest
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import aws.smithy.kotlin.runtime.net.url.Url
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.buildJsonObject
import mu.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * The deployed payments subsystem on LocalStack, as the tests see it: the BFF's REST API, our bus, the stores
 * that show what each service recorded, and what crossed the boundary into the downstream subsystem.
 */
object LocalPayments {
    private val logger = KotlinLogging.logger { }

    private const val ENDPOINT = "http://localhost:4566"
    private const val REGION = "us-east-1"
    const val BUS = "payments-event-hub-local-bus"
    const val BFF_TABLE = "payments-bff-local-payments"
    const val CONTROL_EVENTS_TABLE = "payments-control-service-local-events"
    const val DOWNSTREAM_QUEUE = "downstream-event-hub-local-verification"

    /** One id per test run, so runs never see each other's payments, customers or accounts. */
    val run: String = java.lang.Long.toString(System.currentTimeMillis(), 36)

    val bffUrl: String = System.getenv("PAYMENTS_BFF_URL")?.takeIf { it.isNotBlank() }?.trimEnd('/')
        ?: error("PAYMENTS_BFF_URL is not set: run the tests with examples/payments/run_itests.sh")

    private val credentials = StaticCredentialsProvider { accessKeyId = "test"; secretAccessKey = "test" }

    val dynamoDb by lazy { DynamoDbClient { region = REGION; endpointUrl = Url.parse(ENDPOINT); credentialsProvider = credentials } }
    val eventBridge by lazy { EventBridgeClient { region = REGION; endpointUrl = Url.parse(ENDPOINT); credentialsProvider = credentials } }
    val sqs by lazy { SqsClient { region = REGION; endpointUrl = Url.parse(ENDPOINT); credentialsProvider = credentials } }
    private val http: HttpClient = HttpClient.newHttpClient()

    // ------------------------------------------------------------------------------------------------ BFF API

    data class Response(val status: Int, val body: String) {
        val json: JsonObject get() = Json.parseToJsonElement(body).jsonObject
        fun string(field: String): String? = json[field]?.jsonPrimitive?.content
    }

    fun request(method: String, path: String, customer: String?, body: String? = null): Response {
        val builder = HttpRequest.newBuilder(URI.create(bffUrl + path)).header("Content-Type", "application/json")
        customer?.let { builder.header("X-Customer-Id", it) }
        builder.method(method, body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody())
        val response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
        return Response(response.statusCode(), response.body())
    }

    fun paymentBody(from: String, to: String, amount: String = "100.00", currency: String = "USD"): String {
        val kind = if (to.startsWith("K-")) "card" else "bank"
        return """{"from":{"kind":"checking","account":"$from"},"to":{"kind":"$kind","account":"$to"},"amount":"$amount","currency":"$currency"}"""
    }

    fun submit(id: String, from: String, to: String, customer: String = "u-42", amount: String = "100.00"): Response =
        request("PUT", "/payments/$id", customer, paymentBody(from, to, amount))

    fun view(id: String, customer: String = "u-42"): Response = request("GET", "/payments/$id", customer)

    fun status(id: String, customer: String = "u-42"): String? = view(id, customer).takeIf { it.status == 200 }?.string("status")

    // --------------------------------------------------------------------------------------- the accounts subsystem

    /** Plays the accounts subsystem: an ACCOUNTS.* event onto our bus with source `external`. */
    suspend fun publishAccount(number: String, customer: String, status: String = "open", type: String = "ACCOUNTS.ACCOUNT_OPENED", timestamp: Long = 1730000000000) {
        val detail = buildJsonObject {
            put("id", "acct-$number-$type-$timestamp")
            put("type", type)
            put("timestamp", timestamp)
            put("partitionKey", number)
            put("account", buildJsonObject {
                put("number", number); put("kind", "checking"); put("holderId", customer); put("status", status); put("currency", "USD")
            })
        }
        putEvent(source = "external", detailType = type, detail = detail.toString())
    }

    suspend fun putEvent(source: String, detailType: String, detail: String) {
        val response = eventBridge.putEvents(PutEventsRequest {
            entries = listOf(PutEventsRequestEntry {
                eventBusName = BUS
                this.source = source
                this.detailType = detailType
                this.detail = detail
            })
        })
        response.failedEntryCount shouldBe 0
    }

    /** The BFF's record of an account, once ACCOUNT_CHANGED has reached it. */
    suspend fun bffAccount(number: String): Map<String, AttributeValue>? =
        dynamoDb.getItem(GetItemRequest {
            tableName = BFF_TABLE
            key = mapOf("pk" to AttributeValue.S(number), "sk" to AttributeValue.S("ACCOUNT"))
            consistentRead = true
        }).item

    // -------------------------------------------------------------------------------------------- control service

    /** The payment's events as the control service correlated them: its view of the saga. */
    suspend fun correlated(paymentId: String): List<PaymentsEvent> =
        dynamoDb.query(QueryRequest {
            tableName = CONTROL_EVENTS_TABLE
            keyConditionExpression = "pk = :pk"
            expressionAttributeValues = mapOf(":pk" to AttributeValue.S(paymentId))
            consistentRead = true
        }).items.orEmpty()
            .mapNotNull { (it["event"] as? AttributeValue.S)?.value }
            .map { PaymentsEventCodec.decode(it) as PaymentsEvent }
            .sortedBy { it.timestamp }

    const val LEDGER_TABLE = "payments-core-checking-ingress-gateway-local-ledger"

    /** Core checking's postings for a payment, by transaction type (`PMT` debit, `RFND` refund): a double debit shows here. */
    suspend fun ledgerPostings(paymentId: String, txnType: String): Int =
        dynamoDb.query(QueryRequest {
            tableName = LEDGER_TABLE
            indexName = "byReference"
            keyConditionExpression = "REF_NO = :r AND TXN_TYP = :t"
            expressionAttributeValues = mapOf(":r" to AttributeValue.S(paymentId), ":t" to AttributeValue.S(txnType))
        }).count

    /** LocalStack sweeps expired TTL items periodically; this asks it to do it now. */
    fun expireNow() {
        val response = http.send(
            HttpRequest.newBuilder(URI.create("$ENDPOINT/_aws/dynamodb/expired")).DELETE().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        logger.info { "expired items: ${response.body()}" }
    }

    // ----------------------------------------------------------------------------------- the downstream subsystem

    private val crossed = mutableListOf<JsonObject>()
    private val crossedLock = Mutex()

    /**
     * Everything that has crossed into the downstream bus during this run, as EventBridge delivered it
     * (`source`, `detail-type`, `detail`). Each call drains the verification queue; what it read is kept for
     * every test, since a message can only be received once.
     */
    suspend fun crossedBoundary(): List<JsonObject> = crossedLock.withLock {
        val queueUrl = sqs.getQueueUrl(GetQueueUrlRequest { queueName = DOWNSTREAM_QUEUE }).queueUrl
        while (true) {
            val messages = sqs.receiveMessage(ReceiveMessageRequest {
                this.queueUrl = queueUrl
                maxNumberOfMessages = 10
                waitTimeSeconds = 1
            }).messages.orEmpty()
            if (messages.isEmpty()) break
            for (message in messages) {
                crossed += Json.parseToJsonElement(message.body!!).jsonObject
                sqs.deleteMessage(DeleteMessageRequest { this.queueUrl = queueUrl; receiptHandle = message.receiptHandle })
            }
        }
        crossed.toList()
    }
}

/** Retries [block] until it returns non-null, or fails after [timeout] with the last value seen. */
suspend fun <T : Any> eventually(timeout: Duration = 60.seconds, every: Duration = 2.seconds, what: String, block: suspend () -> T?): T {
    val start = TimeSource.Monotonic.markNow()
    while (true) {
        block()?.let { return it }
        if (start.elapsedNow() > timeout) error("Timed out after $timeout waiting for $what")
        delay(every)
    }
}
