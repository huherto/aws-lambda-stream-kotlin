package org.myorg.payments

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.ConditionalCheckFailedException
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import kotlin.time.Duration

/**
 * Simulation only: core checking being down. Unlike the other fault accounts, an outage is felt by every payment, so
 * its state is shared. A payment starts it at most once, so its own retries don't keep core checking down forever.
 */
/** Simulation only: something that happens the first time for a key, and never again (e.g. C-8's lost request). */
fun interface SimulatedOnce {
    suspend fun firstTime(key: String): Boolean

    companion object {
        fun never() = SimulatedOnce { false }
    }
}

class DynamoDbSimulatedOnce(private val client: DynamoDbClient, private val table: String) : SimulatedOnce {
    override suspend fun firstTime(key: String): Boolean = try {
        client.putItem(PutItemRequest {
            tableName = table
            item = mapOf("pk" to AttributeValue.S("SIMULATION#once#$key"))
            conditionExpression = "attribute_not_exists(pk)"
        })
        true
    } catch (e: ConditionalCheckFailedException) {
        false
    }
}

interface SimulatedOutage {
    suspend fun startOnce(paymentId: String, now: Long)
    suspend fun isDown(now: Long): Boolean

    companion object {
        fun none() = object : SimulatedOutage {
            override suspend fun startOnce(paymentId: String, now: Long) = Unit
            override suspend fun isDown(now: Long) = false
        }
    }
}

class DynamoDbSimulatedOutage(
    private val client: DynamoDbClient,
    private val table: String,
    private val lasts: Duration,
) : SimulatedOutage {

    private val outageKey = mapOf("pk" to AttributeValue.S("SIMULATION#outage"))

    override suspend fun startOnce(paymentId: String, now: Long) {
        val first = try {
            client.putItem(PutItemRequest {
                tableName = table
                item = mapOf("pk" to AttributeValue.S("SIMULATION#outage-started#$paymentId"))
                conditionExpression = "attribute_not_exists(pk)"
            })
            true
        } catch (e: ConditionalCheckFailedException) {
            false
        }
        if (!first) return
        client.putItem(PutItemRequest {
            tableName = table
            item = outageKey + ("until" to AttributeValue.N((now + lasts.inWholeMilliseconds).toString()))
        })
    }

    override suspend fun isDown(now: Long): Boolean {
        val item = client.getItem(GetItemRequest { tableName = table; key = outageKey; consistentRead = true }).item ?: return false
        val until = (item["until"] as? AttributeValue.N)?.value?.toLong() ?: return false
        return now < until
    }
}
