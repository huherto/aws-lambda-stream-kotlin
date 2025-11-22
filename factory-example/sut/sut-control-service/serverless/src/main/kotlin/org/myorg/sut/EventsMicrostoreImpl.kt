package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import java.nio.charset.StandardCharsets
import java.util.stream.Stream
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

@ExperimentalTime
class EventsMicrostoreImpl<T : Thing> : EventsMicrostore<T> {

    private var clock : Clock? = null

    fun tableName() : String {
        var tableName = System.getenv("EVENT_TABLE_NAME")
        if (tableName == null || tableName.isEmpty()) {
            tableName = System.getenv("ENTITY_TABLE_NAME")
        }
        return tableName;
    }

    fun awsRegion() : String {
        return System.getenv("AWS_REGION")
    }

    fun daysInSecs(days: Int?) : Long {
        return if (days == null) { 33 } else { days * 24 * 60 * 60L }
    }

    fun nowInSecs() : Long {
        if (clock == null)
            clock = Clock.System
        return clock!!.now().toEpochMilliseconds() / 1000L;
    }

    fun ttl(start: Long, days: Int?) : Long {
        var d = days;
        if (days == null) { d = 33}

        return start / 1000 + 60 * 60 * 24 * d
    }

    fun expire(start: Long, days: Int?) : String {
        return ttl(start, days).toString()
    }

    fun save(event: KinesisEvent) {
        for (rec in event.getRecords()) {
            val data = rec.getKinesis().getData()
            val payload = StandardCharsets.UTF_8.decode(data).toString()
        }
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
            it.timestamp()?:now.toString()
        }
        val event = uow.event?.let{}

        val itemValues = mapOf(
            "pk" to AttributeValue.builder().s(uow.event.id).build(),
            "sk" to AttributeValue.builder().s("EVENT").build(),
            "discriminator" to AttributeValue.builder().s("EVENT").build(),
            "timestamp" to AttributeValue.builder().n(timeStamp.toString()).build(),
            "awsregion" to AttributeValue.builder().s(awsRegion()).build(),
            "ttl" to AttributeValue.builder().n(ttl.toString()).build(),
            "expire" to AttributeValue.builder().n(expire.toString()).build(),
            "data" to AttributeValue.builder().s(uow.key).build(),
            "event" to AttributeValue.builder().s(uow.event?.toString()).build()
//        expire: rule.expire,
//        data: uow.key,
//        event: rule.includeRaw ?
//        /* istanbul ignore next */
//        uow.event : (0, _omit.default)(uow.event, ['raw'])
        )

        val request = PutItemRequest.builder()
            .tableName(tableName())
            .item(itemValues)
            .build()


    }

}