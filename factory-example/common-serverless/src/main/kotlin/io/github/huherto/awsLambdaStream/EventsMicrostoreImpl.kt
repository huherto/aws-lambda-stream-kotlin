package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue.N
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging
import java.time.Clock

class EventsMicrostoreImpl : EventsMicrostore {

    constructor(dynamoDbClient : DynamoDbClient, clock : Clock = Clock.systemDefaultZone(), envConfig : EnvironmentConfig = EnvironmentConfig()) : super() {
        this.dynamoDbClient = dynamoDbClient
        this.clock = clock
        this.envConfig = envConfig
    }

    private val logger = KotlinLogging.logger {  }

    private var dynamoDbClient : DynamoDbClient

    private var clock : Clock

    private var envConfig : EnvironmentConfig

    fun daysInSecs(days: Int?) : Long {
        return if (days == null) { 33 } else { days * 24 * 60 * 60L }
    }

    fun nowInSecs() : Long {
        return clock.instant().toEpochMilli() / 1000L
    }

    override suspend fun save(flow: Flow<UnitOfWork>, options: EventsMicrostore.SaveOptions) {

        // Should be able to send in batches.
        flow.collect { uow -> save(uow, options) }
    }

    fun nullableS(s: String?) : AttributeValue {
        return s?.let { S(it) } ?: AttributeValue.Null(true)
    }

    suspend fun save(uow: UnitOfWork, ops: EventsMicrostore.SaveOptions) {

        if (uow.event == null) {
            return
        }

        val now = nowInSecs()
        val ttl = now + daysInSecs(90)
        val expire = now + daysInSecs(ops.expireDays)
        val event : Event? = uow.event
        val encodedEvent = event?.encoded()
        val timeStamp = event?.timestamp
        val awsRegion = envConfig.awsRegion()

        val itemValues = mapOf(
            "pk" to nullableS(event?.id),
            "sk" to S("EVENT"),
            "discriminator" to S("EVENT"),
            "timestamp" to N(timeStamp.toString()),
            "awsregion" to S(awsRegion),
            "ttl" to N(ttl.toString()),
            "expire" to N(expire.toString()),
            "data" to nullableS(uow.key),
            "event" to nullableS(encodedEvent)
        )

        val request = PutItemRequest {
            tableName = envConfig.tableName()
            item = itemValues
        }

        dynamoDbClient.putItem(request)

    }

}