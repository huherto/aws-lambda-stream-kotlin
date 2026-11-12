package io.kopipes.tools.replay

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.s3.S3Client
import io.kopipes.aws.tools.ReplayEvents
import kotlinx.coroutines.runBlocking

/**
 * Replays events from the event lake to a function. Region and credentials come from the environment; set
 * `AWS_ENDPOINT_URL` to target LocalStack, e.g. `http://localhost:4566`.
 */
fun main() = runBlocking {
    // A custom endpoint (LocalStack) serves buckets by path, not as subdomains.
    val customEndpoint = !System.getenv("AWS_ENDPOINT_URL").isNullOrBlank()
    S3Client.fromEnvironment { if (customEndpoint) forcePathStyle = true }.use { s3 ->
        LambdaClient.fromEnvironment { }.use { lambda ->
            ReplayEvents().main(
                s3 = s3,
                lambda = lambda,
            )
        }
    }
}
