package org.myorg.example

import software.amazon.awscdk.App

fun main() {
    val app = App()

    println("Working Directory = " + System.getProperty("user.dir"))
    MyStack(app, "template-pipeline-resources")
    app.synth()
    println("Finished CDK App")
}
