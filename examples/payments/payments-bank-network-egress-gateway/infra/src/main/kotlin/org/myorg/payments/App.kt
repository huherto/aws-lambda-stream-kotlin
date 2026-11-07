package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-bank-network-egress-gateway",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    BankNetworkEgressStack(app, serviceProps)
    BankNetworkEgressStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
