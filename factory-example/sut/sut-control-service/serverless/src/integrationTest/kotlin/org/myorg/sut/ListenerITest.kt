package org.myorg.sut

import aws.sdk.kotlin.services.kinesis.model.PutRecordRequest
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.myorg.sut.testsupport.KinesisHelper
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName

// Components tested.
//   - Connection from Kinesis Stream to Lambda Listener.
//   - Listener being able to write to Dynamodb.
//  Possibly also but not sure
//    - Trigger lambda consuming from Dynamodb stream.
//    - Trigger sending messages to EventBridge.
class ListenerITest {
    val localstack: LocalStackContainer = LocalStackContainer(DockerImageName.parse("localstack/localstack:stable"))
        .withServices(LocalStackContainer.Service.KINESIS)
        .withExposedPorts(4566)

    @Test
    fun sendEvents(): Unit {
        localstack.start()
        runBlocking<Unit> {

            val streamName = "my-kinesis-stream"
            val kinesisClient = KinesisHelper.createKinesisClient(localstack)
            kinesisClient.use { kinesis ->
                KinesisHelper.createKinesisStream(kinesis, streamName)
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
    }

}