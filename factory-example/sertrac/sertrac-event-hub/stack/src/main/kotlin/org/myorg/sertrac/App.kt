package org.myorg.sertrac

import software.amazon.awscdk.App

fun main() {
    val app = App()

    println("Working Directory = " + System.getProperty("user.dir"))
    println("Starting CDK App")
    val serviceProps = ServiceProps(
        org = "myorg",
        subsys = "sertrac",
        service = "sertrac-event-hub",
        stage = Stage.DEV,
        region = "us-east-1")
    MyStack(app, serviceProps)
    app.synth()
    println("Finished CDK App")
}
