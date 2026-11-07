package org.myorg.payments

import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Match
import software.amazon.awscdk.services.events.Rule
import software.amazon.awscdk.services.events.targets.EventBus as EventBusTarget
import software.constructs.Construct

/**
 * Decides what the payment subsystem tells the rest of the bank. Its listener turns PAYMENT_COMPLETED and
 * PAYMENT_FAILED into PAYMENTS.* with the bus source `external`; its **egress rule** on our bus sends exactly those
 * to the downstream bus. The rule needs the prefix: our bus also carries the ACCOUNTS.* events that arrive, and
 * matching `source = external` alone would forward them too. One target per downstream subsystem.
 */
class PaymentsEgressGatewayStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val bus = EventBus.fromEventBusName(this, "Bus", busName())

        val listener = javaFunction("listener", "org.myorg.payments.Listener::handleRequest", "payments-egress-gateway.jar", baseLambdaEnvironment())
        bus.grantPutEventsTo(listener)
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))

        val downstream = EventBus.fromEventBusName(this, "DownstreamBus", "downstream-event-hub-${stage()}-bus")
        Rule.Builder.create(this, "EgressRule")
            .ruleName("${service()}-${stage()}-egress-rule")
            .eventBus(bus)
            .eventPattern(
                EventPattern.builder()
                    .source(listOf("external"))
                    .detailType(Match.prefix("PAYMENTS."))
                    .build()
            )
            .targets(listOf(EventBusTarget(downstream)))
            .build()
    }
}
