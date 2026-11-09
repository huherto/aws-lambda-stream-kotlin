package org.myorg.payments

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import aws.smithy.kotlin.runtime.net.url.Url
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import mu.KotlinLogging
import java.util.Base64

/**
 * The records the listener would get from its channel for these requests: a redelivery, as the stream would make.
 * A fault raised while handling them carries this record, so an operator's resubmission replays it to the listener.
 */
fun redeliveryOf(claims: List<Claim>, now: Long): String {
    val records = claims.mapNotNull { claim ->
        val request = claim.request ?: return@mapNotNull null
        buildJsonObject {
            put("eventSource", "aws:kinesis")
            put("eventID", "reconciler:${claim.key}:$now")
            put("kinesis", buildJsonObject {
                put("partitionKey", claim.key.substringBefore('#'))
                put("sequenceNumber", "reconciler-$now")
                put("approximateArrivalTimestamp", now / 1000.0)
                put("data", Base64.getEncoder().encodeToString(request.toByteArray()))
            })
        }
    }
    return buildJsonObject { put("Records", JsonArray(records)) }.toString()
}

/**
 * Requests in doubt don't wait on the stream, so nothing would look at them again. Every minute, the reconciler
 * redelivers each one to the listener, whose called-once logic asks core checking once: confirmed, still in doubt,
 * or (past the grace period) a fault for an operator. It never calls core checking itself.
 */
class Reconciler(
    private val claims: Claims,
    private val redeliver: suspend (payload: String) -> Unit,
    private val clock: () -> Long = System::currentTimeMillis,
    private val batch: Int = 10,
) {
    private val logger = KotlinLogging.logger { }

    suspend fun reconcile(): Int {
        val inDoubt = claims.inDoubt().filter { it.request != null }
        if (inDoubt.isEmpty()) return 0
        logger.info { "redelivering ${inDoubt.size} request(s) in doubt: ${inDoubt.map { it.key }}" }
        inDoubt.chunked(batch).forEach { chunk -> redeliver(redeliveryOf(chunk, clock())) }
        return inDoubt.size
    }

    companion object {
        fun build(): Reconciler {
            val dynamoDb = AwsGlobalRegistry.dynamoDbClientFactory().getClient("reconciler")
            val claims = DynamoDbClaims(dynamoDb, System.getenv("CALLS_TABLE_NAME") ?: error("CALLS_TABLE_NAME is not configured"))
            val listener = System.getenv("LISTENER_FUNCTION_NAME") ?: error("LISTENER_FUNCTION_NAME is not configured")
            val lambda = LambdaClient {
                region = System.getenv("AWS_REGION") ?: "us-east-1"
                System.getenv("AWS_ENDPOINT_URL")?.let { endpointUrl = Url.parse(it) }
            }
            val logger = KotlinLogging.logger { }
            return Reconciler(claims, redeliver = { payload ->
                val response = lambda.invoke(InvokeRequest {
                    functionName = listener
                    invocationType = InvocationType.RequestResponse
                    this.payload = payload.toByteArray()
                })
                // A failed redelivery (core checking down, say) is fine: the next round tries again.
                response.functionError?.let { logger.warn { "redelivery failed ($it): the next round will try again" } }
            })
        }
    }
}

/** Scheduled every minute. */
class ReconcilerHandler(
    factory: () -> Reconciler = { Reconciler.build() },
) : RequestHandler<ScheduledEvent, Int> {
    private val reconciler by loggedLazy("Reconciler", KotlinLogging.logger { }, factory)

    override fun handleRequest(event: ScheduledEvent, context: Context): Int = runBlocking { reconciler.reconcile() }
}
