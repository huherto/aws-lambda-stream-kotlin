package org.myorg.payments

import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.s3.Bucket
import software.constructs.Construct

/**
 * Adds DEPOSIT_REQUESTED with `to.kind = bank` to tonight's transfer file. In simulation mode the file isn't
 * sent: after a delay the simulator drops the settlement file the network would have returned where the ingress
 * gateway picks up files. Locally the delay is 20 s, and 180 s for B-7, past the control service's 2-minute deadline.
 */
class BankNetworkEgressStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val jar = "payments-bank-network-egress-gateway.jar"
        val simulate = simulate()
        var env = baseLambdaEnvironment() + ("SIMULATE" to simulate.toString())

        if (simulate) {
            val inboundName = Partners.bankInboundBucketName(org(), stage(), regionName())
            val inbound = Bucket.fromBucketName(this, "Inbound", inboundName)
            val simulator = javaFunction(
                "simulator", "org.myorg.payments.Simulator::handleRequest", jar,
                env + ("INBOUND_BUCKET_NAME" to inboundName),
            )
            inbound.grantPut(simulator)
            val queue = simulationQueue(simulator)
            env = env + mapOf(
                "SIMULATION_QUEUE_URL" to queue.queueUrl,
                "SIMULATED_DELAY_SECONDS" to "20",
                "SIMULATED_LATE_DELAY_SECONDS" to "180",
            )
            val listener = javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env)
            queue.grantSendMessages(listener)
            consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))
        } else {
            val outbound = Bucket.Builder.create(this, "Outbound")
                .bucketName("${org()}-${service()}-${stage()}-${regionName()}")
                .removalPolicy(RemovalPolicy.RETAIN)
                .build()
            val listener = javaFunction(
                "listener", "org.myorg.payments.Listener::handleRequest", jar,
                env + ("OUTBOUND_BUCKET_NAME" to outbound.bucketName),
            )
            outbound.grantPut(listener)
            consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))
        }
    }
}
