package org.myorg.payments

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.isAwsRetryable
import io.kopipes.aws.flavors.MaterializePipeline
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.flavors.Pipeline

/**
 * An outside system answers with a reference, never with our payment. So each ingress gateway keeps the
 * payments it has asked about, from the request events on the default channel, and joins answers to them.
 * Items: `pk = <payment id>`, `sk = PAYMENT`, `payment = <json>`.
 */
fun requestUpdate(table: String, uow: UnitOfWork): UpdateItemRequest? {
    val payment = (uow.event as? PaymentEvent)?.payment ?: return null
    return UpdateItemRequest {
        tableName = table
        key = mapOf("pk" to AttributeValue.S(payment.id), "sk" to AttributeValue.S("PAYMENT"))
        updateExpression = "SET #p = :p"
        expressionAttributeNames = mapOf("#p" to "payment")
        expressionAttributeValues = mapOf(":p" to AttributeValue.S(paymentsJson.encodeToString(Payment.serializer(), payment)))
    }
}

fun requestsPipeline(context: AwsKopipesContext, id: String, table: String, eventFilter: EventFilter): Pipeline =
    MaterializePipeline.builder()
        .id(id)
        .context(context)
        .eventFilter(eventFilter)
        .toUpdateRequest { uow -> requestUpdate(table, uow) }
        .build()

/** Thrown when an answer arrives before its request was recorded. Retryable: the request is on its way. */
class PaymentNotKnownYet(reference: String) : RuntimeException("No request recorded yet for payment $reference")

fun interface PaymentLookup {
    /** The payment behind an outside system's reference, or [PaymentNotKnownYet]. */
    suspend fun payment(reference: String): Payment
}

class DynamoDbPaymentLookup(
    private val client: DynamoDbClient,
    private val table: String,
) : PaymentLookup {
    override suspend fun payment(reference: String): Payment {
        val item = client.getItem(GetItemRequest {
            tableName = table
            key = mapOf("pk" to AttributeValue.S(reference), "sk" to AttributeValue.S("PAYMENT"))
            consistentRead = true
        }).item
        val json = (item?.get("payment") as? AttributeValue.S)?.value ?: throw PaymentNotKnownYet(reference)
        return paymentsJson.decodeFromString(Payment.serializer(), json)
    }

    companion object {
        fun fromContext(context: AwsKopipesContext): DynamoDbPaymentLookup = DynamoDbPaymentLookup(
            client = context.dynamoDbClientFactory.getClient("lookup"),
            table = System.getenv("REQUESTS_TABLE_NAME") ?: error("REQUESTS_TABLE_NAME is not configured"),
        )
    }
}

/**
 * The context an ingress gateway runs with: [PaymentNotKnownYet] is retryable, so with `STREAM_RETRY_ENABLED=true` the
 * batch is retried by its source (stream or queue) instead of becoming a fault.
 */
fun gatewayContext(): AwsKopipesContext =
    AwsKopipesContext(isRetryable = { it is PaymentNotKnownYet || isAwsRetryable(it) })
