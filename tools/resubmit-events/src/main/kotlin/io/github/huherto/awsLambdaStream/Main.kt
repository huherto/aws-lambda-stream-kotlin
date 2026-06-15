package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.s3.S3Client
import io.github.huherto.awsLambdaStream.tools.resubmit.ResubmitEvents

suspend fun main() {

    val resubmit = ResubmitEvents()

    val argv = resubmit.loadArgs()

    print(argv)

    LambdaClient {
        region = argv.region ?: System.getenv("AWS_REGION")
    }.use { lambda ->
        S3Client {
            region = argv.region ?: System.getenv("AWS_REGION")
        }.use { s3 ->
            resubmit.runResubmitEvents(
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