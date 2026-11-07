package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-core-checking-ingress-gateway",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    CoreCheckingIngressStack(app, serviceProps)
    CoreCheckingIngressStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
