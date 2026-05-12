package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.MaterializePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue.Set
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublishOptions
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import io.github.huherto.awsLambdaStream.sinks.updateExpression
import io.github.huherto.awsLambdaStream.utils.nullableN
import io.github.huherto.awsLambdaStream.utils.nullableS
import java.nio.ByteBuffer

class ListenerContainer(
    val envConfig: EnvironmentConfig,
    val dynamoDbConnector: DynamoDbConnector,
    val faultManager: FaultManager,
) {

    companion object {
        fun build() : ListenerContainer {
            val envConfig = EnvironmentConfig()
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory(envConfig)
            val dynamoDbConnector = DynamoDbConnector(clientFactory = dynamoDbClientFactory)
            val eventPublisherOptions = EventBridgePublishOptions(envConfig)
            val eventPublisher = EventBridgePublisher(eventPublisherOptions)
            val faultManager = FaultManager(envConfig, eventPublisher)
            return ListenerContainer(
                envConfig = envConfig,
                dynamoDbConnector = dynamoDbConnector,
                faultManager = faultManager
            )
        }
    }

    class MyKinesisAdapter : KinesisAdapter() {
        override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
            return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
        }
    }

    val kinesisAdapter: KinesisAdapter by lazy { MyKinesisAdapter() }

    suspend fun toUpdateRequest(uow: UnitOfWork): UpdateItemRequest? {

        val event = uow.event as? TrackedUnitEvent ?: return null
        val entity = event.entity ?: return null
        val entityId = entity.id ?: return null

        return UpdateItemRequest {
            tableName = envConfig.entityTableName()
                ?: error("ENTITY_TABLE_NAME is not configured")

            key = mapOf(
                "pk" to AttributeValue.S(entityId),
                "sk" to AttributeValue.S("SHIPMENT")
            )

            val map = mutableMapOf(
                "senderFullName" to Set(nullableS(entity.senderFullName)),
                "trackingNumber" to Set(nullableS(entity.trackingNumber)),
                "returnAddress" to Set(nullableS(entity.returnAddress?.asJson())),
                "destinationAddress" to Set(nullableS(entity.destinationAddress?.asJson())),
                "weight" to Set(nullableN(entity.weight)),
            )

            entity.dimensions?.let {
                map["dimensions.length"] = Set(nullableN(it.length))
                map["dimensions.width"] = Set(nullableN(it.width))
                map["dimensions.height"] = Set(nullableN(it.height))
            }

            val ue = updateExpression(map)

            updateExpression = ue.updateExpression
            expressionAttributeNames = ue.expressionAttributeNames
            expressionAttributeValues = ue.expressionAttributeValues
        }
    }

    private val materializePipeline: Pipeline by lazy {
        MaterializePipeline(
            pipelineId = "m1",
            envConfig = envConfig,
            eventFilter = EventFilters.classes(TrackedUnitEvent::class),
            toUpdateRequest = ::toUpdateRequest,
            dynamoDbConnector = dynamoDbConnector,
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .faultManager(faultManager)
            .addPipeline(materializePipeline)
            .build()
    }

}