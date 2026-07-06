package org.myorg.sut.facades

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.listObjectsV2
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.smithy.kotlin.runtime.content.decodeToString
import kotlinx.coroutines.delay
import mu.KotlinLogging
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class S3Facade(
    private val config: AwsLocalConfig = AwsLocalConfig(),
) {
    private val logger = KotlinLogging.logger {}

    val client: S3Client by lazy {
        S3Client {
            region = config.region
            endpointUrl = config.endpointUrl
            forcePathStyle = true
            credentialsProvider = config.credentialsProvider()
        }
    }

    suspend fun <T> waitForResult(
        timeout: Duration = 20_000.milliseconds,
        delayBetweenAttempts: Duration = 1_000.milliseconds,
        onTimeout: () -> T?,
        block: suspend S3Client.() -> T?,
    ): T? {
        val startTime = System.currentTimeMillis()

        while (true) {
            val result = client.block()
            if (result != null) {
                return result
            }

            if (System.currentTimeMillis() - startTime > timeout.inWholeMilliseconds) {
                return onTimeout()
            }

            delay(delayBetweenAttempts)
        }
    }

    suspend fun verifyFaultEventStoredInS3(faultId: String): String? =
        waitForResult(
            timeout = 20_000.milliseconds,
            delayBetweenAttempts = 1_000.milliseconds,
            onTimeout = {
                logger.error { "Timed out waiting for s3 object with faultId: $faultId to be inserted." }
                null
            },
        ) {
            val bucketName = "myorg-sut-event-fault-monitor-local-us-east-1"
            val response = listObjectsV2 {
                bucket = bucketName
            }

            val contents = response.contents.orEmpty()

            logger.info {
                "S3 list response: keyCount=${response.keyCount}, " +
                        "isTruncated=${response.isTruncated}, " +
                        "contents=${contents.map { it.key }}"
            }

            val keys = contents.sortedBy { it.key }.map { it.key }.reversed()

            keys.take(5).firstNotNullOfOrNull { key ->
                logger.info { "S3 object key: $key" }

                val content = getObject(GetObjectRequest {
                    bucket = bucketName
                    this.key = key
                }) { s3Response ->
                    val content = s3Response.body?.decodeToString()
                    logger.info { "S3 object content: $content" }
                    content
                }

                if (content != null && content.contains(faultId)) {
                    logger.info { "Fault found in S3 object: $key" }
                    content
                } else {
                    null
                }
            }
        }

    fun close() {
        client.close()
    }
}