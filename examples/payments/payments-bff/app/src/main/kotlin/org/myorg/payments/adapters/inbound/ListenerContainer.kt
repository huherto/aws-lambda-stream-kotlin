package org.myorg.payments.adapters.inbound

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.kopipes.aws.awsEnvConfig
import io.kopipes.aws.connectors.DynamoDbConnector
import io.kopipes.aws.flavors.MaterializePipeline
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.Pipeline
import org.myorg.payments.*
import org.myorg.payments.adapters.outbound.Items

/** The events that move the customer's view. */
val VIEW_EVENTS = setOf(
    PaymentsEvent.FUNDS_WITHDRAWN,
    PaymentsEvent.WITHDRAWAL_FAILED,
    PaymentsEvent.DEPOSIT_REQUESTED,
    PaymentsEvent.FUNDS_DEPOSITED,
    PaymentsEvent.DEPOSIT_FAILED,
    PaymentsEvent.REFUND_REQUESTED,
    PaymentsEvent.FUNDS_REFUNDED,
    PaymentsEvent.PAYMENT_EXPIRED,
    PaymentsEvent.PAYMENT_COMPLETED,
    PaymentsEvent.PAYMENT_FAILED,
)

/**
 * Records that the payment heard this event: `at_<TYPE>` = the event's timestamp (when the fact happened).
 * `if_not_exists` keeps the first one, so a repeat changes nothing and the view never goes backwards.
 */
fun heardUpdate(table: String, uow: UnitOfWork): UpdateItemRequest? {
    val event = uow.event as? PaymentEvent ?: return null
    val type = event.eventType()
    if (type !in VIEW_EVENTS) return null
    val timestamp = event.timestamp ?: error("$type ${event.id} has no timestamp")

    val names = mutableMapOf("#at" to Items.HEARD_PREFIX + type)
    val values = mutableMapOf<String, AttributeValue>(":at" to AttributeValue.N(timestamp.toString()))
    val sets = mutableListOf("#at = if_not_exists(#at, :at)")
    if (event is Failure) {
        names["#reason"] = Items.REASON_PREFIX + type
        values[":reason"] = AttributeValue.S(event.reason)
        sets += "#reason = if_not_exists(#reason, :reason)"
    }

    return UpdateItemRequest {
        tableName = table
        key = Items.key(event.payment.id, Items.PAYMENT)
        updateExpression = "SET " + sets.joinToString(", ")
        expressionAttributeNames = names
        expressionAttributeValues = values
    }
}

/** Keeps the latest known state of an account. An older or repeated event fails the condition and changes nothing. */
fun accountUpdate(table: String, uow: UnitOfWork): UpdateItemRequest? {
    val event = uow.event as? AccountChanged ?: return null
    val timestamp = event.timestamp ?: error("ACCOUNT_CHANGED ${event.id} has no timestamp")
    val account = event.account
    return UpdateItemRequest {
        tableName = table
        key = Items.key(account.number, Items.ACCOUNT)
        updateExpression = "SET #c = :c, #k = :k, #s = :s, #ts = :ts, #d = :d"
        conditionExpression = "attribute_not_exists(#ts) OR #ts < :ts"
        expressionAttributeNames = mapOf(
            "#c" to Items.CUSTOMER_ID, "#k" to "kind", "#s" to "status", "#ts" to "timestamp", "#d" to "discriminator",
        )
        expressionAttributeValues = mapOf(
            ":c" to AttributeValue.S(account.customerId),
            ":k" to AttributeValue.S(account.kind),
            ":s" to AttributeValue.S(account.status),
            ":ts" to AttributeValue.N(timestamp.toString()),
            ":d" to AttributeValue.S(Items.ACCOUNT),
        )
    }
}

class ListenerContainer(
    val tableName: String,
    val dynamoDbConnectorOptions: DynamoDbConnector.Options = DynamoDbConnector.Options(),
) {
    companion object {
        const val VIEW = "view"
        const val ACCOUNTS = "accounts"

        fun build(): ListenerContainer = ListenerContainer(
            tableName = awsEnvConfig().entityTableName() ?: error("ENTITY_TABLE_NAME is not configured"),
        )
    }

    val viewPipeline: Pipeline by lazy {
        MaterializePipeline.builder()
            .id(VIEW)
            .eventFilter(EventFilters.onContent { it.eventType() in VIEW_EVENTS })
            .toUpdateRequest { uow -> heardUpdate(tableName, uow) }
            .dynamoDbConnectorOptions(dynamoDbConnectorOptions)
            .build()
    }

    val accountsPipeline: Pipeline by lazy {
        MaterializePipeline.builder()
            .id(ACCOUNTS)
            .eventFilter(EventFilters.name(PaymentsEvent.ACCOUNT_CHANGED))
            .toUpdateRequest { uow -> accountUpdate(tableName, uow) }
            .dynamoDbConnectorOptions(dynamoDbConnectorOptions)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.builder()
            .addPipeline(viewPipeline)
            .addPipeline(accountsPipeline)
            .build()
    }

    val kinesisAdapter: KinesisAdapter by lazy { KinesisAdapter(eventCodec = PaymentsEventCodec) }
}
