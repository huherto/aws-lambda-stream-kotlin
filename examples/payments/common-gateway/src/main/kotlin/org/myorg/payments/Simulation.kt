package org.myorg.payments

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.SendMessageRequest
import aws.smithy.kotlin.runtime.net.url.Url

/**
 * Simulation mode of the egress gateways (PaymentsServiceSpecs.md › Simulation mode). Off by default and never
 * on in production. With it on, the egress gateway doesn't contact the external system: it produces the response
 * that system would have sent, in its own format, and delivers it to the ingress gateway's real input.
 * The outcome depends only on the account.
 */
object Simulation {
    /** Withdrawal rejected: insufficient funds. */
    const val INSUFFICIENT_FUNDS_ACCOUNT = "C-2"

    /** Card credit fails: card closed. */
    const val CLOSED_CARD = "K-3"

    /** Bank transfer rejected: no such account. */
    const val UNKNOWN_BANK_ACCOUNT = "B-8"

    /** Bank transfer settles, but after the control service's deadline. */
    const val LATE_BANK_ACCOUNT = "B-7"

    // Fault accounts: the outcome is still success, but the external system misbehaves on the way.

    /** Core checking applies the debit, but the call times out: the answer is lost. */
    const val TIMEOUT_AFTER_DEBIT_ACCOUNT = "C-7"

    /** The card processor delivers the same webhook twice. */
    const val DUPLICATE_WEBHOOK_CARD = "K-6"

    /** The bank network drops the same settlement file twice. */
    const val DUPLICATE_FILE_BANK = "B-6"

    /** How many times the external system delivers its answer for this account. */
    fun deliveries(account: String): Int = if (account == DUPLICATE_WEBHOOK_CARD || account == DUPLICATE_FILE_BANK) 2 else 1

    /** Seconds between repeated deliveries of the same answer. */
    const val REDELIVERY_SECONDS = 3

    fun enabled(): Boolean = System.getenv("SIMULATE")?.equals("true", ignoreCase = true) == true

    fun delaySeconds(default: Int): Int = System.getenv("SIMULATED_DELAY_SECONDS")?.toIntOrNull() ?: default
}

/** Delivers the simulated response later: an SQS message with a delay (up to 15 minutes) for the simulator. */
fun interface DelayedDelivery {
    suspend fun deliverLater(body: String, delaySeconds: Int)
}

class SqsDelayedDelivery(
    private val client: SqsClient,
    private val queueUrl: String,
) : DelayedDelivery {
    override suspend fun deliverLater(body: String, delaySeconds: Int) {
        client.sendMessage(SendMessageRequest {
            queueUrl = this@SqsDelayedDelivery.queueUrl
            messageBody = body
            this.delaySeconds = delaySeconds.coerceIn(0, 900)
        })
    }

    companion object {
        fun fromEnvironment(): SqsDelayedDelivery {
            val endpoint = System.getenv("AWS_ENDPOINT_URL")
            val client = SqsClient {
                region = System.getenv("AWS_REGION") ?: "us-east-1"
                endpoint?.let { endpointUrl = Url.parse(it) }
            }
            return SqsDelayedDelivery(client, System.getenv("SIMULATION_QUEUE_URL") ?: error("SIMULATION_QUEUE_URL is not configured"))
        }
    }
}
