package org.myorg.payments

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.ConditionalCheckFailedException
import aws.sdk.kotlin.services.dynamodb.model.DeleteItemRequest
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import kotlinx.coroutines.delay
import mu.KotlinLogging
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** The call's outcome is unknown: it may or may not have been applied. */
class CallOutcomeUnknown(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** The call certainly never reached core checking (for example, the connection was refused). */
class CallNotSent(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** A call is in doubt and core checking doesn't show the posting yet: try again later. Retryable. */
class CallInDoubt(message: String) : RuntimeException(message)

/** A call has stayed in doubt past the grace period: someone must check core checking. Becomes a fault. */
class CallUnresolved(message: String) : RuntimeException(message)

/** One claim per request: `<payment>#PMT` or `<payment>#RFND`. */
data class Claim(val key: String, val state: String, val claimedAt: Long) {
    companion object {
        const val CALLING = "CALLING"
        const val SENT = "SENT"
    }
}

interface Claims {
    /** Claims [key] if nobody has; otherwise returns the existing claim. */
    suspend fun claim(key: String, now: Long): Claim?
    suspend fun markSent(key: String)
    suspend fun release(key: String)
}

/**
 * "Called once" for a system without idempotency keys. It isn't possible to guarantee exactly one call to such a
 * system, so the gateway guarantees the next best thing: **it never calls twice blindly**.
 *
 * 1. Claim the request (`CALLING`) before calling; mark it `SENT` after.
 * 2. A repeat that finds `SENT` does nothing.
 * 3. A repeat that finds `CALLING` is in doubt: we called and lost the answer, or crashed. It asks core checking
 *    whether the posting exists. Yes: `SENT`. Not yet: retry later. Past [grace]: a fault, for an operator.
 * 4. A call that certainly never left ([CallNotSent]) releases the claim, so the next attempt calls normally.
 */
class CalledOnce(
    private val coreChecking: CoreChecking,
    private val claims: Claims,
    private val grace: Duration = 300.seconds,
    private val inquiryWait: Duration = 10.seconds,
    private val inquiryEvery: Duration = 1.seconds,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private val logger = KotlinLogging.logger { }

    suspend fun debit(payment: Payment) = once(payment, LedgerEntry.PAYMENT) { coreChecking.debit(payment) }

    suspend fun credit(payment: Payment) = once(payment, LedgerEntry.REFUND) { coreChecking.credit(payment) }

    private suspend fun once(payment: Payment, txnType: String, call: suspend () -> Unit) {
        val key = "${payment.id}#$txnType"
        val existing = claims.claim(key, clock())

        if (existing == null) {
            try {
                call()
            } catch (e: CallNotSent) {
                claims.release(key)
                throw e
            } catch (e: Exception) {
                throw CallOutcomeUnknown("$key: core checking call failed, outcome unknown", e)
            }
            claims.markSent(key)
            return
        }

        when (existing.state) {
            Claim.SENT -> logger.info { "$key was already sent: not calling core checking again" }
            else -> resolve(existing, payment, txnType)
        }
    }

    private suspend fun resolve(claim: Claim, payment: Payment, txnType: String) {
        val deadline = clock() + inquiryWait.inWholeMilliseconds
        while (true) {
            if (coreChecking.posted(payment, txnType)) {
                logger.info { "${claim.key} was in doubt; core checking shows the posting: not calling again" }
                claims.markSent(claim.key)
                return
            }
            if (clock() >= deadline) break
            delay(inquiryEvery)
        }
        if (clock() - claim.claimedAt > grace.inWholeMilliseconds) {
            throw CallUnresolved("${claim.key}: in doubt since ${claim.claimedAt} and core checking shows no posting; check it before retrying")
        }
        throw CallInDoubt("${claim.key}: in doubt and core checking shows no posting yet")
    }
}

/** The claims, in the egress gateway's own table. */
class DynamoDbClaims(private val client: DynamoDbClient, private val table: String) : Claims {

    private fun key(key: String) = mapOf("pk" to AttributeValue.S(key))

    override suspend fun claim(key: String, now: Long): Claim? = try {
        client.putItem(PutItemRequest {
            tableName = table
            item = key(key) + mapOf(
                "state" to AttributeValue.S(Claim.CALLING),
                "claimedAt" to AttributeValue.N(now.toString()),
            )
            conditionExpression = "attribute_not_exists(pk)"
        })
        null
    } catch (e: ConditionalCheckFailedException) {
        val item = client.getItem(GetItemRequest { tableName = table; this.key = key(key); consistentRead = true }).item
            ?: error("$key: claim vanished")
        Claim(
            key = key,
            state = (item["state"] as AttributeValue.S).value,
            claimedAt = (item["claimedAt"] as AttributeValue.N).value.toLong(),
        )
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
}
