package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-event-hub",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    EventHubStack(app, serviceProps)
    EventHubStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
