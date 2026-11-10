package org.myorg.payments.adapters.outbound

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.ConditionalCheckFailedException
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import mu.KotlinLogging
import org.myorg.payments.application.BreakerState
import org.myorg.payments.application.BreakerStore

/** The breaker's state in the egress gateway's `calls` table, one item, written with optimistic concurrency. */
class DynamoDbBreakerStore(
    private val client: DynamoDbClient,
    private val table: String,
    private val name: String = "BREAKER#core-checking",
) : BreakerStore {

    private val key = mapOf("pk" to AttributeValue.S(name))

    override suspend fun read(): BreakerState {
        val item = client.getItem(GetItemRequest { tableName = table; key = this@DynamoDbBreakerStore.key; consistentRead = true }).item
            ?: return BreakerState()
        fun n(name: String) = (item[name] as? AttributeValue.N)?.value?.toLong() ?: 0
        return BreakerState(
            state = (item["state"] as? AttributeValue.S)?.value ?: BreakerState.CLOSED,
            failures = n("failures").toInt(),
            until = n("until"),
            closedAt = n("closedAt"),
            opens = n("opens").toInt(),
            version = n("version"),
        )
    }

    override suspend fun write(next: BreakerState, expectedVersion: Long): Boolean = try {
        client.putItem(PutItemRequest {
            tableName = table
            item = key + mapOf(
                "state" to AttributeValue.S(next.state),
                "failures" to AttributeValue.N(next.failures.toString()),
                "until" to AttributeValue.N(next.until.toString()),
                "closedAt" to AttributeValue.N(next.closedAt.toString()),
                "opens" to AttributeValue.N(next.opens.toString()),
                "version" to AttributeValue.N(next.version.toString()),
            )
            conditionExpression = "attribute_not_exists(pk) OR #v = :v"
            expressionAttributeNames = mapOf("#v" to "version")
            expressionAttributeValues = mapOf(":v" to AttributeValue.N(expectedVersion.toString()))
        })
        true
    } catch (e: ConditionalCheckFailedException) {
        false
    }
}
