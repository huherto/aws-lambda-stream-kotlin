package org.myorg.payments

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.ConditionalCheckFailedException
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import kotlinx.coroutines.delay
import mu.KotlinLogging

/** The card processor said `429 Too Many Requests`. Retryable: a retried credit is the same credit (idempotency key). */
class RateLimited(val retryAfterMillis: Long, message: String = "card processor rate limit reached") : RuntimeException(message)

/** Counts uses of a per-second budget. */
interface RateStore {
    /** Takes one unit of the budget for second [epochSecond] if fewer than [limit] were taken. */
    suspend fun take(epochSecond: Long, limit: Int): Boolean
}

/**
 * The processor's allowed rate, shared by every instance of the gateway, whichever shard it reads: a fixed budget per
 * second. With no token left this second, a call waits for the next one; the backlog waits on the stream.
 *
 * At high rates a single item per second gets hot; the refinement is to lease tokens in blocks (not built).
 */
class TokenBucket(
    private val store: RateStore,
    private val perSecond: Int,
    private val clock: () -> Long = System::currentTimeMillis,
    private val sleep: suspend (Long) -> Unit = { delay(it) },
) {
    suspend fun acquire() {
        while (true) {
            val now = clock()
            val second = now / 1000
            if (store.take(second, perSecond)) return
            sleep((second + 1) * 1000 - now)
        }
    }
}

/**
 * Calls the card processor at its allowed rate. A `429` is a safety net, not the plan: it means our pace and the
 * processor's disagree (clocks, window edges), so wait as told and try again, a few times, before handing the request
 * back to the stream.
 */
class PacedCardProcessor(
    private val processor: CardProcessor,
    private val bucket: TokenBucket,
    private val maxAttempts: Int = 3,
    private val sleep: suspend (Long) -> Unit = { delay(it) },
) : CardProcessor {
    private val logger = KotlinLogging.logger { }

    override suspend fun credit(payment: Payment) {
        var attempt = 1
        while (true) {
            bucket.acquire()
            try {
                processor.credit(payment)
                return
            } catch (e: RateLimited) {
                logger.warn { "${payment.id}: 429 from the card processor (attempt $attempt), retrying after ${e.retryAfterMillis} ms" }
                if (attempt++ >= maxAttempts) throw e
                sleep(e.retryAfterMillis)
            }
        }
    }
}

/** Per-second budgets in the gateway's `rate` table; items expire after a minute. */
class DynamoDbRateStore(
    private val client: DynamoDbClient,
    private val table: String,
    private val name: String,
) : RateStore {

    override suspend fun take(epochSecond: Long, limit: Int): Boolean = try {
        client.updateItem(UpdateItemRequest {
            tableName = table
            key = mapOf("pk" to AttributeValue.S("$name#$epochSecond"))
            updateExpression = "ADD #u :one SET #ttl = :ttl"
            conditionExpression = "attribute_not_exists(#u) OR #u < :limit"
            expressionAttributeNames = mapOf("#u" to "used", "#ttl" to "ttl")
            expressionAttributeValues = mapOf(
                ":one" to AttributeValue.N("1"),
                ":limit" to AttributeValue.N(limit.toString()),
                ":ttl" to AttributeValue.N((epochSecond + 60).toString()),
            )
        })
        true
    } catch (e: ConditionalCheckFailedException) {
        false
    }

    /** Adds one to a plain counter, e.g. how many 429s the simulated processor returned. */
    suspend fun count(counter: String) {
        client.updateItem(UpdateItemRequest {
            tableName = table
            key = mapOf("pk" to AttributeValue.S(counter))
            updateExpression = "ADD #n :one"
            expressionAttributeNames = mapOf("#n" to "count")
            expressionAttributeValues = mapOf(":one" to AttributeValue.N("1"))
        })
    }

    suspend fun counted(counter: String): Long =
        (client.getItem(GetItemRequest { tableName = table; key = mapOf("pk" to AttributeValue.S(counter)); consistentRead = true })
            .item?.get("count") as? AttributeValue.N)?.value?.toLong() ?: 0
}
