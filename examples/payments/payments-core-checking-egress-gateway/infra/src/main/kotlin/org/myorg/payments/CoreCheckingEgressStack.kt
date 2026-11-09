package org.myorg.payments

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Billing
import software.amazon.awscdk.services.dynamodb.TableV2
import software.amazon.awscdk.services.events.Rule
import software.amazon.awscdk.services.events.Schedule
import software.amazon.awscdk.services.events.targets.LambdaFunction
import software.constructs.Construct

/**
 * Calls core checking for WITHDRAWAL_REQUESTED and REFUND_REQUESTED, **once**: each request is claimed in the
 * `calls` table before the call, and a request in doubt is resolved by asking core checking, never by calling again.
 * Requests in doubt don't hold up the others: a reconciler redelivers them every minute until they are resolved or,
 * past the grace period, become faults.
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
            // How long a call may stay in doubt before it becomes a fault for an operator. Short locally, for demos.
            "CALL_GRACE_SECONDS" to if (stage() == Stage.LOCAL) "60" else "300",
            // Circuit breaker: open after 3 failures in a row; try again after the cool-down.
            "BREAKER_THRESHOLD" to "3",
            "BREAKER_OPEN_SECONDS" to if (stage() == Stage.LOCAL) "15" else "30",
            // When core checking is closed, e.g. "22:00-06:00@America/New_York". Not set locally.
            "CORE_CHECKING_CLOSED_HOURS" to (node.tryGetContext("coreCheckingClosedHours")?.toString() ?: ""),
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
                // C-5 takes simulated core checking down for everyone, for this long.
                "SIMULATED_OUTAGE_SECONDS" to "60",
            )
            // An invocation may sleep up to 20 s for an open breaker, then spend up to 10 s on an inquiry.
            javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env, timeoutSeconds = 60).also { listener ->
                queue.grantSendMessages(listener)
                ledger.grantReadData(listener)
            }
        } else {
            val url = node.tryGetContext("coreCheckingUrl")?.toString() ?: "https://core-checking.invalid"
            javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env + ("CORE_CHECKING_URL" to url), timeoutSeconds = 60)
        }

        calls.grantReadWriteData(listener)
        // While core checking is down, requests wait on the channel rather than being dropped.
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT), retryAttempts = 10_000)

        // Requests in doubt don't wait on the channel: every minute the reconciler redelivers them to the listener.
        val reconciler = javaFunction(
            "reconciler", "org.myorg.payments.ReconcilerHandler::handleRequest", jar,
            baseLambdaEnvironment() + mapOf(
                "CALLS_TABLE_NAME" to calls.tableName,
                "LISTENER_FUNCTION_NAME" to listener.functionName,
            ),
            timeoutSeconds = 60,
        )
        calls.grantReadData(reconciler)
        listener.grantInvoke(reconciler)
        Rule.Builder.create(this, "ReconcileEveryMinute")
            .ruleName("${service()}-${stage()}-reconcile")
            .schedule(Schedule.rate(Duration.minutes(1)))
            .targets(listOf(LambdaFunction(reconciler)))
            .build()
    }
}
