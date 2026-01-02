package io.github.huherto.`aws-lambda-stream`.testsupport

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.delay
import aws.sdk.kotlin.services.kinesis.KinesisClient
import aws.sdk.kotlin.services.kinesis.model.CreateStreamRequest
import aws.sdk.kotlin.services.kinesis.model.DescribeStreamRequest
import aws.sdk.kotlin.services.kinesis.model.ResourceNotFoundException
import aws.sdk.kotlin.services.kinesis.model.StreamStatus
import org.testcontainers.containers.localstack.LocalStackContainer

class KinesisHelper {

    companion object {

        fun createKinesisClient(localstack : LocalStackContainer): KinesisClient {
            // val endpoint = localstack.getEndpointOverride(LocalStackContainer.Service.KINESIS)
            val endpoint = localstack.endpoint
            return KinesisClient {
                this.region = localstack.region
                this.endpointUrl = Url.parse(endpoint.toString())
                this.credentialsProvider =
                    StaticCredentialsProvider {
                        this.accessKeyId = localstack.accessKey
                        this.secretAccessKey = localstack.secretKey
                    }
            }
        }

        suspend fun createKinesisStream(kinesisClient: KinesisClient, streamName: String) {
            val createStreamRequest = CreateStreamRequest {
                this.streamName = streamName
                this.shardCount = 1
            }
            try {
                kinesisClient.createStream(createStreamRequest)
                println("Creating stream '$streamName'...")
                waitForStreamToBeActive(kinesisClient, streamName)
            } catch (e: Exception) {
                // Handle cases where the stream might already exist
                if (e.message?.contains("already exists") == true) {
                    println("Stream '$streamName' already exists. Skipping creation.")
                    waitForStreamToBeActive(kinesisClient, streamName)
                } else {
                    throw e
                }
            }
        }

        suspend fun waitForStreamToBeActive(kinesisClient: KinesisClient, streamName: String) {
            val describeStreamRequest = DescribeStreamRequest {
                this.streamName = streamName
            }

            var streamStatus: StreamStatus? = null
            val startTime = System.currentTimeMillis()
            val endTime = startTime + (10 * 60 * 1000) // 10 minute timeout

            while (System.currentTimeMillis() < endTime) {
                try {
                    val response = kinesisClient.describeStream(describeStreamRequest)
                    streamStatus = response.streamDescription?.streamStatus
                    if (streamStatus == StreamStatus.Active) {
                        println("Stream '$streamName' is ACTIVE.")
                        return
                    }
                } catch (e: ResourceNotFoundException) {
                    // Stream might not be found immediately after creation call
                }
                delay(1000) // Wait for 1 second before rechecking
            }
            if (streamStatus != StreamStatus.Active) {
                throw RuntimeException("Stream '$streamName' never went active, current status: $streamStatus")
            }
        }

    }

}