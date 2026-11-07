package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-bank-network-ingress-gateway",
        stage = Stage.DEV,
        region = "us-east-1",
    )
    BankNetworkIngressStack(app, serviceProps)
    BankNetworkIngressStack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
}
