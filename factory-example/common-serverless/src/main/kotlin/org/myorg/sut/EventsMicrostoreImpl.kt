package org.myorg.sut

//import software.amazon.awssdk.services.dynamodb.DynamoDbClient
//import software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN
//import software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS
//import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue.N
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import kotlinx.coroutines.runBlocking
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
        return clock.instant().toEpochMilli() / 1000L
    }

    override fun save(stream: Stream<UnitOfWork<E>>, options: EventsMicrostore.SaveOptions) {

        // Should be able to send in batches.
        stream.forEach { uow -> save(uow, options) }
    }

    fun nullableS(s: String?) : AttributeValue {
        return s?.let { S(it) } ?: AttributeValue.Null(true)
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
            "pk" to nullableS(event?.id),
            "sk" to S("EVENT"),
            "discriminator" to S("EVENT"),
            "timestamp" to N(timeStamp.toString()),
            "awsregion" to S(awsRegion),
            "ttl" to N(ttl.toString()),
            "expire" to N(expire.toString()),
            "data" to nullableS(uow.key),
            "event" to nullableS(event?.toString())
        )


        val request = PutItemRequest {
            tableName = envConfig.tableName()
            item = itemValues
        }

        // This is here just to be able to call a suspend function.
        // Possibly all should be thought out to be used as co-routines,
        // but I am still learning how to do that.
        runBlocking {
            dynamoDbClient.putItem(request)
        }

    }

}