package org.myorg.sut.facades

import aws.sdk.kotlin.services.sns.SnsClient
import aws.sdk.kotlin.services.sns.model.ListTopicsRequest
import aws.sdk.kotlin.services.sns.model.PublishRequest

class SnsFacade(
    private val config: AwsLocalConfig = AwsLocalConfig(),
) {
    val client: SnsClient by lazy {
        SnsClient {
            region = config.region
            endpointUrl = config.endpointUrl
            credentialsProvider = config.credentialsProvider()
        }
    }

    suspend fun publishToTopic(
        topicNameContains: String,
        message: String,
        subject: String,
        messageGroupId: String,
        messageDeduplicationId: String,
    ): String {
        val topicArn = client.listTopics(ListTopicsRequest {})
            .topics
            .orEmpty()
            .mapNotNull { it.topicArn }
            .firstOrNull { it.contains(topicNameContains) }
            ?: error("SNS topic not found containing: $topicNameContains")

        val response = client.publish(PublishRequest {
            this.topicArn = topicArn
            this.message = message
            this.subject = subject
            this.messageGroupId = messageGroupId
            this.messageDeduplicationId = messageDeduplicationId
        })

        return response.messageId ?: error("SNS publish did not return a message id")
    }

    fun close() {
        client.close()
    }
}