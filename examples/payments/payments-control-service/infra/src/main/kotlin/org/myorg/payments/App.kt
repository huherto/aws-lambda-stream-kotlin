package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-control-service",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    ControlServiceStack(app, serviceProps)
    ControlServiceStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
