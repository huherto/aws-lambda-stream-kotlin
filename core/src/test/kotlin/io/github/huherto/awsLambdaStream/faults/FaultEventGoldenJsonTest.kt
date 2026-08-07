package io.github.huherto.awsLambdaStream.faults

import io.github.huherto.awsLambdaStream.serialization.JacksonSerializationStrategy
import io.github.huherto.awsLambdaStream.serialization.KotlinxSerializationStrategy
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import org.junit.jupiter.api.Test

class FaultEventGoldenJsonTest {

    private val jackson = JacksonSerializationStrategy()
    private val kotlinx = KotlinxSerializationStrategy()

    @Test
    fun `fault event should produce stable JSON structure`() {
        val event = FaultEvent().apply {
            id = "fault-123"
            type = "fault"
            timestamp = 123456789L
            partitionKey = "pk-1"
            tags = mapOf(
                "functionname" to "target-lambda",
                "pipeline" to "target-pipeline"
            )
            err = ErrorSnapshot(
                name = "IllegalStateException",
                message = "Something failed"
            )
            uow = UnitOfWorkSnapshot(
                pipeline = PipelineSnapshot(id = "target-pipeline"),
                key = "pk-1",
                sequenceNumber = "123456",
                shardId = "shardId-000000000000",
                record = ReplayRecordSnapshot(
                    kind = "kinesis",
                    payload = Json.parseToJsonElement("""
                        {
                            "eventID": "shardId-000000000000:1",
                            "eventName": "aws:kinesis:record",
                            "eventSource": "aws:kinesis",
                            "awsRegion": "us-east-1",
                            "kinesis": {
                              "partitionKey": "pk-1",
                              "sequenceNumber": "123456",
                              "data": "base64-payload"
                            }
                        }
                    """).let { it as JsonObject }
                ),
                event = EventSummarySnapshot(
                    id = "original-event-id",
                    type = "shipment-created",
                    partitionKey = "pk-1"
                )
            )
        }

        val jacksonJson = jackson.serialize(event)
        val kotlinxJson = kotlinx.serialize(event)

        println("Jackson:\n$jacksonJson")
        println("Kotlinx:\n$kotlinxJson")

        // Compare normalized versions
        Json.parseToJsonElement(jacksonJson) shouldBe Json.parseToJsonElement(kotlinxJson)
    }
}
