package org.myorg.payments.adapters.outbound

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.ConditionalCheckFailedException
import aws.sdk.kotlin.services.dynamodb.model.DeleteItemRequest
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.ScanRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import org.myorg.payments.application.Claim
import org.myorg.payments.application.Claims

/** The claims, in the egress gateway's own table. */
class DynamoDbClaims(private val client: DynamoDbClient, private val table: String) : Claims {

    private fun key(key: String) = mapOf("pk" to AttributeValue.S(key))

    private fun claimOf(key: String, item: Map<String, AttributeValue>) = Claim(
        key = key,
        state = (item["state"] as AttributeValue.S).value,
        claimedAt = (item["claimedAt"] as AttributeValue.N).value.toLong(),
        request = (item["request"] as? AttributeValue.S)?.value,
    )

    override suspend fun find(key: String): Claim? =
        client.getItem(GetItemRequest { tableName = table; this.key = key(key); consistentRead = true }).item?.let { claimOf(key, it) }

    override suspend fun claim(key: String, now: Long, request: String?): Claim? = try {
        client.putItem(PutItemRequest {
            tableName = table
            item = key(key) + listOfNotNull(
                "state" to AttributeValue.S(Claim.CALLING),
                "claimedAt" to AttributeValue.N(now.toString()),
                request?.let { "request" to AttributeValue.S(it) },
            ).toMap()
            conditionExpression = "attribute_not_exists(pk)"
        })
        null
    } catch (e: ConditionalCheckFailedException) {
        find(key) ?: error("$key: claim vanished")
    }

    override suspend fun markSent(key: String) {
        client.updateItem(UpdateItemRequest {
            tableName = table
            this.key = key(key)
            updateExpression = "SET #s = :s"
            expressionAttributeNames = mapOf("#s" to "state")
            expressionAttributeValues = mapOf(":s" to AttributeValue.S(Claim.SENT))
        })
    }

    override suspend fun release(key: String) {
        client.deleteItem(DeleteItemRequest { tableName = table; this.key = key(key) })
    }

    override suspend fun inDoubt(): List<Claim> {
        val claims = mutableListOf<Claim>()
        var startKey: Map<String, AttributeValue>? = null
        do {
            val page = client.scan(ScanRequest {
                tableName = table
                filterExpression = "#s = :calling"
                expressionAttributeNames = mapOf("#s" to "state")
                expressionAttributeValues = mapOf(":calling" to AttributeValue.S(Claim.CALLING))
                exclusiveStartKey = startKey
            })
            page.items.orEmpty().forEach { item -> claims += claimOf((item["pk"] as AttributeValue.S).value, item) }
            startKey = page.lastEvaluatedKey?.takeIf { it.isNotEmpty() }
        } while (startKey != null)
        return claims
    }
}
