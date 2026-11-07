package org.myorg.payments

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.kopipes.core.utils.omit
import java.nio.ByteBuffer
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

/** Builds stream records shaped like the items the framework's events microstore writes. */
object StreamRecords {

    private val sequence = AtomicLong(1)

    private fun s(value: String) = AttributeValue().withS(value)
    private fun n(value: Long) = AttributeValue().withN(value.toString())
    private fun bool(value: Boolean) = AttributeValue().withBOOL(value)

    private fun ttlOf(event: PaymentsEvent) = (event.timestamp ?: 0) / 1000 + 33 * 86_400

    /** The item the listener's collect pipeline writes: `pk = <event id>`, `sk = EVENT`. */
    fun eventItem(event: PaymentsEvent): Map<String, AttributeValue> = mapOf(
        "pk" to s(event.id!!),
        "sk" to s("EVENT"),
        "discriminator" to s("EVENT"),
        "timestamp" to n(event.timestamp!!),
        "ttl" to n(ttlOf(event)),
        "expire" to bool(false),
        "suffix" to s(""),
        "data" to s(event.partitionKey!!),
        "event" to s(event.toString()),
    )

    /** The item a correlate pipeline writes: `pk = <payment><suffix>`, `sk = <event id>`, without `raw`. */
    fun correlationItem(
        event: PaymentEvent,
        suffix: String = "",
        ttl: Long = ttlOf(event),
        expire: Boolean = false,
    ): Map<String, AttributeValue> = mapOf(
        "pk" to s(event.payment.id + suffix),
        "sk" to s(event.id!!),
        "discriminator" to s("CORREL"),
        "timestamp" to n(event.timestamp!!),
        "ttl" to n(ttl),
        "expire" to bool(expire),
        "suffix" to s(suffix),
        "data" to s(event.id!!),
        "event" to s(omit(event, "raw")),
    )

    fun insert(image: Map<String, AttributeValue>): DynamodbEvent.DynamodbStreamRecord =
        record("INSERT", newImage = image, oldImage = null)

    /** A deletion by the table's TTL: only the old image, and the service as the identity. */
    fun ttlRemove(image: Map<String, AttributeValue>): DynamodbEvent.DynamodbStreamRecord =
        record("REMOVE", newImage = null, oldImage = image)

    private fun record(
        eventName: String,
        newImage: Map<String, AttributeValue>?,
        oldImage: Map<String, AttributeValue>?,
    ): DynamodbEvent.DynamodbStreamRecord {
        val image = newImage ?: oldImage!!
        val seq = sequence.getAndIncrement().toString()
        return DynamodbEvent.DynamodbStreamRecord().apply {
            this.eventID = "record-$seq"
            this.eventName = eventName
            this.eventSource = "aws:dynamodb"
            this.awsRegion = "us-east-1"
            this.dynamodb = StreamRecord().apply {
                this.keys = mapOf("pk" to image["pk"]!!, "sk" to image["sk"]!!)
                this.newImage = newImage
                this.oldImage = oldImage
                this.sequenceNumber = seq
                this.approximateCreationDateTime = Date()
            }
        }
    }

    fun dynamodbEvent(vararg records: DynamodbEvent.DynamodbStreamRecord): DynamodbEvent =
        DynamodbEvent().apply { this.records = records.toList() }

    fun kinesisEvent(vararg events: PaymentsEvent): KinesisEvent = KinesisEvent().apply {
        records = events.map { event ->
            val seq = sequence.getAndIncrement().toString()
            KinesisEvent.KinesisEventRecord().apply {
                eventID = "shardId-000000000000:$seq"
                eventSource = "aws:kinesis"
                kinesis = KinesisEvent.Record().apply {
                    partitionKey = event.partitionKey
                    sequenceNumber = seq
                    approximateArrivalTimestamp = Date()
                    data = ByteBuffer.wrap(event.toString().toByteArray())
                }
            }
        }
    }
}
