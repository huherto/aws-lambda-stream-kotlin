package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-egress-gateway",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    PaymentsEgressGatewayStack(app, serviceProps)
    PaymentsEgressGatewayStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
