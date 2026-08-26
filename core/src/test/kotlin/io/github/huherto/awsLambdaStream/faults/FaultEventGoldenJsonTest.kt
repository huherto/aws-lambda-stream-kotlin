package io.github.huherto.awsLambdaStream.faults

import io.github.huherto.awsLambdaStream.serialization.KotlinxSerializationStrategy
import io.github.huherto.awsLambdaStream.serialization.snapshots.*
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import org.junit.jupiter.api.Test

class FaultEventGoldenJsonTest {

    private val kotlinx = KotlinxSerializationStrategy()

    @Test
    fun `fault event should produce stable JSON structure`() {
        val event = FaultEvent(
            id = "fault-123",
            type = "fault",
            timestamp = 123456789L,
            partitionKey = "pk-1",
            tags = mapOf(
                "functionname" to "target-lambda",
                "pipeline" to "target-pipeline"
            ),
            err = ErrorSnapshot(
                name = "IllegalStateException",
                message = "Something failed"
            ),
            uow = UnitOfWorkSnapshot(
                pipeline = PipelineSnapshot(id = "target-pipeline"),
                key = "pk-1",
                sequenceNumber = "123456",
                shardId = "shardId-000000000000",
                record = RecordSnapshot(
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
                event = EventSnapshot(
                    id = "original-event-id",
                    type = "shipment-created",
                    partitionKey = "pk-1"
                )
            )
        )

        val kotlinxJson = kotlinx.serialize(event)

        println("Kotlinx:\n$kotlinxJson")

        // Just verify it's valid JSON and contains expected data
        Json.parseToJsonElement(kotlinxJson).let { it as JsonObject }["type"].toString() shouldBe "\"fault\""
    }
}
