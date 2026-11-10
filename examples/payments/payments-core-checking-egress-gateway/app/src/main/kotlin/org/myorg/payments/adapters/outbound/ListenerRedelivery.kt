package org.myorg.payments.adapters.outbound

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import mu.KotlinLogging
import org.myorg.payments.application.Claim
import org.myorg.payments.application.Redelivery
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

/** Redelivers by invoking the listener function with a Kinesis-shaped event. */
class ListenerRedelivery(
    private val invoke: suspend (payload: String) -> Unit,
    private val clock: () -> Long = System::currentTimeMillis,
) : Redelivery {

    override suspend fun redeliver(claims: List<Claim>) = invoke(redeliveryOf(claims, clock()))

    companion object {
        private val logger = KotlinLogging.logger { }

        fun lambda(listener: String): ListenerRedelivery {
            val lambda = LambdaClient {
                region = System.getenv("AWS_REGION") ?: "us-east-1"
                System.getenv("AWS_ENDPOINT_URL")?.let { endpointUrl = Url.parse(it) }
            }
            return ListenerRedelivery(invoke = { payload ->
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
