package org.myorg.payments

import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Billing
import software.amazon.awscdk.services.dynamodb.TableV2
import software.constructs.Construct

/**
 * Calls core checking for WITHDRAWAL_REQUESTED and REFUND_REQUESTED, **once**: each request is claimed in the
 * `calls` table before the call, and a request in doubt is resolved by asking core checking, never by calling again.
 *
 * In simulation mode it calls no one: a delayed message makes the simulator write the ledger row core checking would
 * have written to the ingress gateway's feed, and the inquiry reads that ledger.
 */
class CoreCheckingEgressStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val jar = "payments-core-checking-egress-gateway.jar"
        val simulate = simulate()

        val calls = TableV2.Builder.create(this, "Calls")
            .tableName("${service()}-${stage()}-calls")
            .partitionKey(Attribute.builder().name("pk").type(AttributeType.STRING).build())
            .billing(Billing.onDemand())
            .removalPolicy(RemovalPolicy.RETAIN)
            .build()

        var env = baseLambdaEnvironment() + mapOf(
            "SIMULATE" to simulate.toString(),
            "CALLS_TABLE_NAME" to calls.tableName,
            // A request in doubt is retried by the stream, not turned into a fault, until the grace period is over.
            "STREAM_RETRY_ENABLED" to "true",
            "CALL_GRACE_SECONDS" to "300",
        )

        val listener = if (simulate) {
            val ledgerName = Partners.ledgerTableName(stage())
            val ledger = TableV2.fromTableName(this, "Ledger", ledgerName)
            val simulator = javaFunction(
                "simulator", "org.myorg.payments.Simulator::handleRequest", jar,
                env + ("LEDGER_TABLE_NAME" to ledgerName),
            )
            ledger.grantWriteData(simulator)
            val queue = simulationQueue(simulator)
            env = env + mapOf(
                "SIMULATION_QUEUE_URL" to queue.queueUrl,
                "SIMULATED_DELAY_SECONDS" to "2",
                "LEDGER_TABLE_NAME" to ledgerName,
            )
            javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env).also { listener ->
                queue.grantSendMessages(listener)
                ledger.grantReadData(listener)
            }
        } else {
            val url = node.tryGetContext("coreCheckingUrl")?.toString() ?: "https://core-checking.invalid"
            javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env + ("CORE_CHECKING_URL" to url))
        }

        calls.grantReadWriteData(listener)
        // Requests wait on the channel rather than being dropped; the grace period bounds how long one can wait.
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT), retryAttempts = 10_000)
    }
}
