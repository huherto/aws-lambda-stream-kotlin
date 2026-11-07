package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "downstream",
        service = "downstream-event-hub",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    DownstreamEventHubStack(app, serviceProps)
    DownstreamEventHubStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
