package org.myorg.payments

import software.amazon.awscdk.Duration
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.constructs.Construct

/** Ingress channel (ACCOUNTS.*) → listener → ACCOUNT_CHANGED on our bus. */
class AccountsIngressGatewayStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    init {
        val bus = EventBus.fromEventBusName(this, "Bus", busName())

        val listener = Function.Builder.create(this, "listener")
            .functionName("${service()}-${stage()}-listener")
            .code(Code.fromAsset("../app/build/libs/payments-accounts-ingress-gateway.jar"))
            .handler("org.myorg.payments.Listener::handleRequest")
            .timeout(Duration.seconds(30))
            .memorySize(1024)
            .runtime(Runtime.JAVA_21)
            .environment(baseLambdaEnvironment())
            .build()

        bus.grantPutEventsTo(listener)
        consumeFromKinesis(listener, importChannel("IngressChannel", Channels.INGRESS))
    }
}
