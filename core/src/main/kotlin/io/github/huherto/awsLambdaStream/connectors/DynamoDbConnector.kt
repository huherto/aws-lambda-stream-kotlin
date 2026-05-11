package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork


interface DynamoDbClientFactory : ClientFactory<DynamoDbClient>

class DefaultDynamoDbClientFactory(private val envConfig: EnvironmentConfig) : DynamoDbClientFactory, AbstractClientFactory<DynamoDbClient>() {
    override fun create(): DynamoDbClient {
        val endpointUrl = envConfig.endPointUrl()
        val region = envConfig.awsRegion()
        return DynamoDbClient {
            this.region = region
            this.credentialsProvider = EnvironmentCredentialsProvider()

            // If an endpoint URL is provided (like http://localhost:4566), use it
            endpointUrl?.let { this.endpointUrl = Url.parse(it) }

        }
    }
}

class DynamoDbConnector(
    val debug: (Any?) -> Unit = {},
    val throwConditionFailure: Boolean = false,
    private val clientFactory: DynamoDbClientFactory,
) {

    fun getClient(uow: UnitOfWork): DynamoDbClient {
        val pipelineId = uow.pipeline?.id ?: "unknown"
        return clientFactory.getClient(pipelineId)
    }

    suspend fun update(
        updateRequest: UpdateItemRequest,
        uow: UnitOfWork,
    ): UpdateItemResponse? {
        return try {
            val client = getClient(uow)
            sendCommand() {
                client.updateItem(updateRequest)
            }
        } catch (error: ConditionalCheckFailedException) {
            if (throwConditionFailure) {
                throw error
            }
            null
        }
    }

    suspend fun put(
        putRequest: PutItemRequest,
        uow: UnitOfWork,
    ): PutItemResponse {

        val client = getClient(uow)
        return sendCommand() {
            client.putItem(putRequest)
        }
    }

    private suspend fun <T> sendCommand(
        block: suspend () -> T,
    ): T {

        return try {
            val response = block()
            debug(response)
            response
        } catch (error: Throwable) {
            debug(error)
            throw error
        }
    }
}


