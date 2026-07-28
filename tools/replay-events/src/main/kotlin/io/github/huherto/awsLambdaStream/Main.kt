package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.s3.S3Client
import io.github.huherto.awsLambdaStream.tools.ReplayEvents
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    S3Client.fromEnvironment { }.use { s3 ->
        LambdaClient.fromEnvironment { }.use { lambda ->
            ReplayEvents().main(
                s3 = s3,
                lambda = lambda,
            )
        }
    }
}