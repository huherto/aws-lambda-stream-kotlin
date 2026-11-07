package org.myorg.payments

import software.amazon.awscdk.App

fun main() {
    val app = App()

    println("Working Directory = " + System.getProperty("user.dir"))
    println("Starting CDK App")
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "payments",
        service = "payments-event-lake",
        stage = Stage.DEV,
        region = "us-east-1"
    )
    EventLakeS3Stack(app, serviceProps)
    EventLakeS3Stack(app, serviceProps.copy(stage = Stage.LOCAL))
    app.synth()
    println("Finished CDK App")
}
