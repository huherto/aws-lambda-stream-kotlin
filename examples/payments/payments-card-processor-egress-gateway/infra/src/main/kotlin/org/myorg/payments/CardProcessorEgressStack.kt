package org.myorg.payments

import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Billing
import software.amazon.awscdk.services.dynamodb.TableV2
import software.amazon.awscdk.services.ssm.StringParameter
import software.constructs.Construct

/**
 * Credits cards for DEPOSIT_REQUESTED with `to.kind = card`, at the processor's allowed rate: every call takes a token
 * from a per-second budget in the `rate` table, shared by all instances, so the backlog waits instead of getting 429s.
 *
 * In simulation mode it calls no one: the simulated processor enforces its own limit (and counts the 429s it would
 * return), then, after a delay, the simulator posts the signed webhook the processor would have sent to the ingress
 * gateway's real URL, signed with its real secret.
 */
class CardProcessorEgressStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val jar = "payments-card-processor-egress-gateway.jar"
        val simulate = simulate()

        val rate = TableV2.Builder.create(this, "Rate")
            .tableName("${service()}-${stage()}-rate")
            .partitionKey(Attribute.builder().name("pk").type(AttributeType.STRING).build())
            .billing(Billing.onDemand())
            .timeToLiveAttribute("ttl")
            .removalPolicy(RemovalPolicy.DESTROY)
            .build()

        val processorRate = node.tryGetContext("cardProcessorRate")?.toString() ?: "5"
        var env = baseLambdaEnvironment() + mapOf(
            "SIMULATE" to simulate.toString(),
            "RATE_TABLE_NAME" to rate.tableName,
            // Credits per second the processor allows us.
            "CARD_PROCESSOR_RATE" to processorRate,
            // A 429 the in-invocation retries couldn't absorb goes back to the stream, not to the faults.
            "STREAM_RETRY_ENABLED" to "true",
        )

        val listener = if (simulate) {
            val webhookUrl = StringParameter.valueForStringParameter(this, Partners.cardWebhookUrlParameter(stage()))
            val secret = StringParameter.valueForStringParameter(this, Partners.cardWebhookSecretParameter(stage()))
            val simulator = javaFunction(
                "simulator", "org.myorg.payments.Simulator::handleRequest", jar,
                env + mapOf("WEBHOOK_URL" to webhookUrl, "WEBHOOK_SECRET" to secret),
            )
            val queue = simulationQueue(simulator)
            env = env + mapOf(
                "SIMULATION_QUEUE_URL" to queue.queueUrl,
                "SIMULATED_DELAY_SECONDS" to "5",
                // The simulated processor's own limit: the same as the rate we were given, as in a real contract.
                "SIMULATED_PROCESSOR_LIMIT" to processorRate,
            )
            javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env, timeoutSeconds = 60)
                .also { queue.grantSendMessages(it) }
        } else {
            val url = node.tryGetContext("cardProcessorUrl")?.toString() ?: "https://card-processor.invalid"
            javaFunction(
                "listener", "org.myorg.payments.Listener::handleRequest", jar,
                env + mapOf("CARD_PROCESSOR_URL" to url, "CARD_PROCESSOR_API_KEY" to "configure-me"),
                timeoutSeconds = 60,
            )
        }

        rate.grantReadWriteData(listener)
        // Small batches: each invocation is paced by the budget, so a big batch would take too long.
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT), retryAttempts = 10_000, batchSize = 10)
    }
}
