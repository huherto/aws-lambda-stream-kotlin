package io.kopipes.tools.resubmit

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.s3.S3Client
import io.kopipes.aws.tools.ResubmitFaults

/**
 * Resubmits faults to the functions they came from. Region and credentials come from the environment (or the
 * `region` argument); set `AWS_ENDPOINT_URL` to target LocalStack, e.g. `http://localhost:4566`.
 */
suspend fun main() {

    val resubmit = ResubmitFaults()

    val argv = resubmit.loadArgs()
    // A custom endpoint (LocalStack) serves buckets by path, not as subdomains.
    val customEndpoint = !System.getenv("AWS_ENDPOINT_URL").isNullOrBlank()

    LambdaClient.fromEnvironment {
        argv.region?.let { region = it }
    }.use { lambda ->
        S3Client.fromEnvironment {
            argv.region?.let { region = it }
            if (customEndpoint) forcePathStyle = true
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
