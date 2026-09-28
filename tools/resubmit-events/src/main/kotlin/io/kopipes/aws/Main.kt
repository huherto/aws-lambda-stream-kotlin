package io.kopipes.aws

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.s3.S3Client
import io.kopipes.aws.tools.ResubmitFaults

suspend fun main() {

    val resubmit = ResubmitFaults()

    val argv = resubmit.loadArgs()

    LambdaClient {
        region = argv.region ?: System.getenv("AWS_REGION")
    }.use { lambda ->
        S3Client {
            region = argv.region ?: System.getenv("AWS_REGION")
        }.use { s3 ->
            resubmit.runResubmitFaults(
                argv = argv,
                s3 = s3,
                lambda = lambda,
            )
        }
    }

    println("======================================")
    println("Running time (minutes): ${resubmit.runtimeMinutes()}")
    println("Gap: ${resubmit.counters.list - resubmit.counters.get}")
    println("Final Counters:")
    print(resubmit.counters)
    println("======================================")
}