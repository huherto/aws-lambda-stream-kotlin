package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.kinesis.KinesisClient
import aws.sdk.kotlin.services.kinesis.model.*
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test

// Components tested.
//   - Connection from Kinesis Stream to Lambda Listener.
//   - Listener being able to write to Dynamodb.
//  Possibly also but not sure
//    - Trigger lambda consuming from Dynamodb stream.
//    - Trigger sending messages to EventBridge.

class ListenerITest {

    @Test
    fun sendEvents() = kotlinx.coroutines.runBlocking {

        val streamName = "my-kinesis-stream"
        val kinesisClient = createKinesisClient()
        createKinesisStream(kinesisClient, streamName)
        kinesisClient.use { kinesis ->
            val payload = """{"event":"create","id":"abc123"}"""
            val resp = kinesis.putRecord(
                PutRecordRequest {
                    this.streamName = streamName
                    this.partitionKey = "user-42"
                    this.data = payload.encodeToByteArray()
                }
            )

            println("PutRecord shardId=${resp.shardId} sequenceNumber=${resp.sequenceNumber}")
        }
    }

    fun createKinesisClient(): KinesisClient {
        val localstackEndpoint = "http://localhost:4566"
        val region = "us-east-1"
        return KinesisClient {
            this.region = region
            this.endpointUrl = Url.parse(localstackEndpoint)
            this.credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "abc123"
                    this.secretAccessKey = "secret123"
                    this.sessionToken = "sessionToken"
                }
        }
    }

    suspend fun createKinesisStream(kinesisClient: KinesisClient, streamName: String) {
        val createStreamRequest = CreateStreamRequest{
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