package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamViewType
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test
import java.util.*

class DynamodbStreamRecordReplayJsonTest {

    @Test
    fun `should round trip a dynamodb stream record`() {
        val original = streamRecord()

        val decoded = DynamodbStreamRecordReplayJson.decode(DynamodbStreamRecordReplayJson.encode(original))

        decoded shouldBe original
    }

    @Test
    fun `should keep the record envelope dynamodb streams delivers`() {
        val json = DynamodbStreamRecordReplayJson.encode(streamRecord())

        json shouldContain """"eventID""""
        json shouldContain """"dynamodb""""
        json shouldContain """"NewImage""""
        json shouldContain """"OldImage""""
        // The canonical type envelope survives, so nothing about the image is guessed on the way back.
        json shouldContain """"S""""
        json.contains(""""Records"""") shouldBe false
    }

    @Test
    fun `should round trip an empty record`() {
        val decoded = DynamodbStreamRecordReplayJson.decode(
            DynamodbStreamRecordReplayJson.encode(DynamodbEvent.DynamodbStreamRecord())
        )

        decoded shouldBe DynamodbEvent.DynamodbStreamRecord()
    }

    companion object {
        fun streamRecord(): DynamodbEvent.DynamodbStreamRecord =
            DynamodbEvent.DynamodbStreamRecord().apply {
                eventID = "event-1"
                eventName = "MODIFY"
                eventSource = "aws:dynamodb"
                eventVersion = "1.1"
                awsRegion = "us-east-1"
                eventSourceARN =
                    "arn:aws:dynamodb:us-east-1:123456789012:table/example/stream/2024-01-01T00:00:00.000"
                dynamodb = StreamRecord().apply {
                    approximateCreationDateTime = Date(1_700_000_000_000L)
                    sequenceNumber = "49590338271490256608559692538361571095921575989136588898"
                    sizeBytes = 42L
                    setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                    keys = mapOf("pk" to AttributeValue().withS("shipment-1"))
                    newImage = mapOf(
                        "pk" to AttributeValue().withS("shipment-1"),
                        "timestamp" to AttributeValue().withN("1700000000"),
                        "deleted" to AttributeValue().withBOOL(false),
                    )
                    oldImage = mapOf(
                        "pk" to AttributeValue().withS("shipment-1"),
                        "timestamp" to AttributeValue().withN("1699999999"),
                    )
                }
            }
    }
}
