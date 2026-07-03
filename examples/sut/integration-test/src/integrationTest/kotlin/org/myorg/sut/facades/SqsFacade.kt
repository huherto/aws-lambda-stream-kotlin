package org.myorg.sut.facades

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.*
import kotlinx.coroutines.delay
import mu.KotlinLogging
import kotlin.time.Duration.Companion.milliseconds

class SqsFacade(
    private val config: AwsLocalConfig = AwsLocalConfig(),
) {
    private val logger = KotlinLogging.logger {}

    val client: SqsClient by lazy {
        SqsClient {
            region = config.region
            endpointUrl = config.endpointUrl
            credentialsProvider = config.credentialsProvider()
        }
    }

    suspend fun purgeQueue(queueName: String) {
        val queueUrl = client.getQueueUrl(GetQueueUrlRequest {
            this.queueName = queueName
        }).queueUrl ?: error("Queue URL not found for queue: $queueName")

        client.purgeQueue(PurgeQueueRequest {
            this.queueUrl = queueUrl
        })
    }

    suspend fun verifyNotificationSentToSns(
        queueName: String,
        expectedContent: String? = null,
    ): String? {
        val queueUrl = client.getQueueUrl(GetQueueUrlRequest {
            this.queueName = queueName
        }).queueUrl ?: error("Queue URL not found for queue: $queueName")

        val startTime = System.currentTimeMillis()

        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                logger.error { "Timed out waiting for SNS notification containing: $expectedContent" }
                return null
            }

            val response = client.receiveMessage(ReceiveMessageRequest {
                this.queueUrl = queueUrl
                maxNumberOfMessages = 10
                waitTimeSeconds = 1
            })

            logger.info { "response is ${response.messages?.size} messages" }

            response.messages.orEmpty().forEach { message ->
                deleteMessage(message, queueUrl)

                val body = message.body
                if (body != null) {
                    if (expectedContent != null) {
                        val found = body.contains(expectedContent)
                        logger.info { "finding '$expectedContent' in $body. found=$found" }

                        if (found) {
                            return body
                        }
                    } else {
                        return body
                    }
                }
            }

            logger.info { "no messages found" }

            delay(1000.milliseconds)
        }
    }

    private suspend fun deleteMessage(message: Message, queueUrl: String) {
        message.receiptHandle?.let { receiptHandle ->
            client.deleteMessage(DeleteMessageRequest {
                this.queueUrl = queueUrl
                this.receiptHandle = receiptHandle
            })
        }
    }

    fun close() {
        client.close()
    }
}