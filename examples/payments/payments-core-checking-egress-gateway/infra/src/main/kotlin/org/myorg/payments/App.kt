package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-core-checking-egress-gateway",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    CoreCheckingEgressStack(app, serviceProps)
    CoreCheckingEgressStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
