package org.myorg.payments

import software.amazon.awscdk.services.dynamodb.TableV2
import software.constructs.Construct

/**
 * Calls core checking for WITHDRAWAL_REQUESTED and REFUND_REQUESTED. In simulation mode it calls no one: a delayed
 * message makes the simulator write the ledger row core checking would have written to the ingress gateway's feed.
 */
class CoreCheckingEgressStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val jar = "payments-core-checking-egress-gateway.jar"
        val simulate = simulate()
        var env = baseLambdaEnvironment() + ("SIMULATE" to simulate.toString())

        if (simulate) {
            val ledger = TableV2.fromTableName(this, "Ledger", Partners.ledgerTableName(stage()))
            val simulator = javaFunction(
                "simulator", "org.myorg.payments.Simulator::handleRequest", jar,
                env + ("LEDGER_TABLE_NAME" to Partners.ledgerTableName(stage())),
            )
            ledger.grantWriteData(simulator)
            val queue = simulationQueue(simulator)
            env = env + mapOf("SIMULATION_QUEUE_URL" to queue.queueUrl, "SIMULATED_DELAY_SECONDS" to "2")
            val listener = javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env)
            queue.grantSendMessages(listener)
            consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))
        } else {
            val url = node.tryGetContext("coreCheckingUrl")?.toString() ?: "https://core-checking.invalid"
            val listener = javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env + ("CORE_CHECKING_URL" to url))
            consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))
        }
    }
}
