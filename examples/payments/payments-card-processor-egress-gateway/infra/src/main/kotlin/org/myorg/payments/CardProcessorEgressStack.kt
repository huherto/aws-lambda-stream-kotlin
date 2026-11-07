package org.myorg.payments

import software.amazon.awscdk.services.ssm.StringParameter
import software.constructs.Construct

/**
 * Credits cards for DEPOSIT_REQUESTED with `to.kind = card`. In simulation mode it calls no one: after a delay the
 * simulator posts the signed webhook the processor would have sent to the ingress gateway's real URL, signed with
 * its real secret (both read from the parameters the ingress stack publishes).
 */
class CardProcessorEgressStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val jar = "payments-card-processor-egress-gateway.jar"
        val simulate = simulate()
        var env = baseLambdaEnvironment() + ("SIMULATE" to simulate.toString())

        if (simulate) {
            val webhookUrl = StringParameter.valueForStringParameter(this, Partners.cardWebhookUrlParameter(stage()))
            val secret = StringParameter.valueForStringParameter(this, Partners.cardWebhookSecretParameter(stage()))
            val simulator = javaFunction(
                "simulator", "org.myorg.payments.Simulator::handleRequest", jar,
                env + mapOf("WEBHOOK_URL" to webhookUrl, "WEBHOOK_SECRET" to secret),
            )
            val queue = simulationQueue(simulator)
            env = env + mapOf("SIMULATION_QUEUE_URL" to queue.queueUrl, "SIMULATED_DELAY_SECONDS" to "5")
            val listener = javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env)
            queue.grantSendMessages(listener)
            consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))
        } else {
            val url = node.tryGetContext("cardProcessorUrl")?.toString() ?: "https://card-processor.invalid"
            val listener = javaFunction(
                "listener", "org.myorg.payments.Listener::handleRequest", jar,
                env + mapOf("CARD_PROCESSOR_URL" to url, "CARD_PROCESSOR_API_KEY" to "configure-me"),
            )
            consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))
        }
    }
}
