package org.myorg.payments

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.ConditionalCheckFailedException
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Core checking is down or closed: requests wait. Retryable, so the stream holds them. */
class CoreCheckingUnavailable(message: String) : RuntimeException(message)

/** The breaker's state, as stored. */
data class BreakerState(
    val state: String = CLOSED,
    /** Consecutive failures while closed. */
    val failures: Int = 0,
    /** While open: when a trial call may be made. While half-open: when the trial started. */
    val until: Long = 0,
    /** When core checking last came back (the breaker last closed after being open). */
    val closedAt: Long = 0,
    /** How many times the breaker has opened: what an operator (or a test) looks at. */
    val opens: Int = 0,
    /** Optimistic concurrency: every write must be based on the version it read. */
    val version: Long = 0,
) {
    companion object {
        const val CLOSED = "CLOSED"
        const val OPEN = "OPEN"
        const val HALF_OPEN = "HALF_OPEN"
    }
}

interface BreakerStore {
    suspend fun read(): BreakerState

    /** Writes [next] only if the stored version is still [expectedVersion]. */
    suspend fun write(next: BreakerState, expectedVersion: Long): Boolean
}

/** Hours when core checking is closed, e.g. `22:00-06:00@America/New_York`. Blank: always open. */
class ClosedHours(private val from: LocalTime, private val to: LocalTime, private val zone: ZoneId) {

    companion object {
        fun parse(spec: String?): ClosedHours? {
            if (spec.isNullOrBlank()) return null
            val (hours, zone) = spec.split("@", limit = 2).let { it[0] to ZoneId.of(it.getOrElse(1) { "UTC" }) }
            val (from, to) = hours.split("-", limit = 2).map { LocalTime.parse(it.trim()) }
            return ClosedHours(from, to, zone)
        }
    }

    fun isClosed(epochMillis: Long): Boolean {
        val time = Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalTime()
        return if (from <= to) time >= from && time < to else time >= from || time < to
    }
}

/**
 * A circuit breaker in front of core checking. Closed: calls go through, failures are counted. After [threshold]
 * consecutive failures it opens: no calls for [openFor]. Then it is half-open: exactly one call is let through as a
 * trial; success closes it, failure opens it again. During [closedHours] it is open without any failure.
 */
class Breaker(
    private val store: BreakerStore,
    private val threshold: Int = 3,
    private val openFor: Duration = 30.seconds,
    private val closedHours: ClosedHours? = null,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private val logger = KotlinLogging.logger { }

    /** May a call be made now? `null` if yes; otherwise when to try again (epoch ms). */
    suspend fun admit(): Long? {
        val now = clock()
        if (closedHours?.isClosed(now) == true) return now + openFor.inWholeMilliseconds

        val current = store.read()
        return when (current.state) {
            BreakerState.CLOSED -> null
            BreakerState.OPEN ->
                if (now < current.until) current.until
                // The cool-down is over: whoever wins this write makes the trial call; the others keep waiting.
                else if (store.write(current.copy(state = BreakerState.HALF_OPEN, until = now, version = current.version + 1), current.version)) null
                else now + openFor.inWholeMilliseconds
            // A trial is under way. If it got lost (its function died), allow a new one after a cool-down.
            else -> if (now - current.until > openFor.inWholeMilliseconds) {
                if (store.write(current.copy(until = now, version = current.version + 1), current.version)) null
                else now + openFor.inWholeMilliseconds
            } else current.until + openFor.inWholeMilliseconds
        }
    }

    suspend fun succeeded() {
        val current = store.read()
        if (current.state == BreakerState.CLOSED && current.failures == 0) return
        val cameBack = current.state != BreakerState.CLOSED
        val next = current.copy(
            state = BreakerState.CLOSED,
            failures = 0,
            closedAt = if (cameBack) clock() else current.closedAt,
            version = current.version + 1,
        )
        if (store.write(next, current.version) && cameBack) logger.warn { "core checking is back: breaker closed" }
    }

    suspend fun failed() {
        val current = store.read()
        val failures = current.failures + 1
        val opens = current.state == BreakerState.HALF_OPEN || (current.state == BreakerState.CLOSED && failures >= threshold)
        val next = when {
            opens -> current.copy(
                state = BreakerState.OPEN, failures = 0, until = clock() + openFor.inWholeMilliseconds,
                opens = current.opens + 1, version = current.version + 1,
            )
            current.state == BreakerState.CLOSED -> current.copy(failures = failures, version = current.version + 1)
            else -> return
        }
        if (store.write(next, current.version) && opens) logger.warn { "core checking is failing: breaker open until ${next.until}" }
    }

    /** When core checking last came back: a request in doubt gets its full grace period from then. */
    suspend fun closedAt(): Long = store.read().closedAt

    companion object {
        /** No breaker: always closed. */
        fun alwaysClosed(): Breaker = Breaker(object : BreakerStore {
            override suspend fun read() = BreakerState()
            override suspend fun write(next: BreakerState, expectedVersion: Long) = true
        }, threshold = Int.MAX_VALUE)
    }
}

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
