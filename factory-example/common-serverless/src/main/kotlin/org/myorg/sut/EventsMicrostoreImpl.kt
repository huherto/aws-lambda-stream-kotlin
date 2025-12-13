package org.myorg.sut

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import java.time.Clock
import java.util.stream.Stream

class EventsMicrostoreImpl<E : Event> : EventsMicrostore<E> {

    constructor(dynamoDbClient : DynamoDbClient, clock : Clock, envConfig : EnvironmentConfig) : super() {
        this.dynamoDbClient = dynamoDbClient
        this.clock = clock
        this.envConfig = envConfig
    }

    private var dynamoDbClient : DynamoDbClient

    private var clock : Clock

    private var envConfig : EnvironmentConfig

    fun daysInSecs(days: Int?) : Long {
        return if (days == null) { 33 } else { days * 24 * 60 * 60L }
    }

    fun nowInSecs() : Long {
        return clock.instant().toEpochMilli() / 1000L;
    }

    override fun save(stream: Stream<UnitOfWork<E>>, options: EventsMicrostore.SaveOptions) {
        stream.forEach { uow -> save(uow, options) }
    }

    fun save(uow: UnitOfWork<E>, ops: EventsMicrostore.SaveOptions) {

        if (uow.event == null) {
            return
        }

        val now = nowInSecs()
        val ttl = now + daysInSecs(90)
        val expire = now + daysInSecs(ops.expireDays)
        val event : E? = uow.event
        val timeStamp = event?.timestamp
        val awsRegion = envConfig.awsRegion()

        val itemValues = mapOf(
            "pk" to fromS(event?.id),
            "sk" to fromS("EVENT"),
            "discriminator" to fromS("EVENT"),
            "timestamp" to fromN(timeStamp.toString()),
            "awsregion" to fromS(awsRegion),
            "ttl" to fromN(ttl.toString()),
            "expire" to fromN(expire.toString()),
            "data" to fromS(uow.key),
            "event" to fromS(event?.toString())
        )

        val request = PutItemRequest.builder()
            .tableName(envConfig.tableName())
            .item(itemValues)
            .build()

        dynamoDbClient.putItem(request)
    }

}