package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-bff",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    PaymentBffStack(app, serviceProps)
    PaymentBffStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
