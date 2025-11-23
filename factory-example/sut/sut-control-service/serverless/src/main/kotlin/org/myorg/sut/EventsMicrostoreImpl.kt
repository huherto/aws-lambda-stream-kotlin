package org.myorg.sut

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import java.util.stream.Stream
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

@ExperimentalTime
class EventsMicrostoreImpl<T : Thing> : EventsMicrostore<T> {

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
        return clock.now().toEpochMilliseconds() / 1000L;
    }

    fun ttl(start: Long, days: Int?) : Long {
        var d = days;
        if (days == null) { d = 33}

        return start / 1000 + 60 * 60 * 24 * d
    }

    fun expire(start: Long, days: Int?) : String {
        return ttl(start, days).toString()
    }

    override fun save(stream: Stream<UnitOfWork<T>>, options: EventsMicrostore.SaveOptions) {
        stream.forEach { uow -> save(uow, options) }
    }


    fun save(uow: UnitOfWork<T>, ops: EventsMicrostore.SaveOptions) {

        if (uow.event == null) {
            return
        }

        val now = nowInSecs()
        val ttl = now + daysInSecs(90)
        val expire = now + daysInSecs(90)
        val timeStamp = uow.event?.let{
            it.timestamp?:now.toString()
        }
        val awsRegion = envConfig.awsRegion()
        val event = uow.event

        val itemValues = mapOf(
            "pk" to fromS(uow.event?.id),
            "sk" to fromS("EVENT"),
            "discriminator" to fromS("EVENT"),
            "timestamp" to fromS(timeStamp),
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