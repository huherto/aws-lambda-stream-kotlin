package io.github.huherto.awsLambdaStream.serialization

import io.github.huherto.awsLambdaStream.DynamodbRaw
import io.github.huherto.awsLambdaStream.RAW_DYNAMODB
import io.github.huherto.awsLambdaStream.RawRecord
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.from.TableChangeEvent
import io.github.huherto.awsLambdaStream.serialization.aws.DynamodbStreamRecordReplayJson
import io.github.huherto.awsLambdaStream.serialization.aws.DynamodbStreamRecordReplayJsonTest
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.Test

/**
 * A replay fixture is only useful if what it captures can be fed back to a Lambda. These pin down
 * that both the source record and `event.raw` come out as JSON rather than a Java `toString()`.
 */
class UnitOfWorkSnapshotTest {

    @Test
    fun `should encode the source record as replayable json`() {
        val record = DynamodbStreamRecordReplayJsonTest.streamRecord()

        val snapshot = UnitOfWork(record = record).toSnapshot()

        val recordSnapshot = snapshot.record.shouldNotBeNull()
        recordSnapshot.kind shouldBe "dynamodb"
        val recordJson = Json.encodeToString(recordSnapshot.payload)
        DynamodbStreamRecordReplayJson.decode(recordJson) shouldBe record
    }

    @Test
    fun `should encode event raw as a discriminated raw record`() {
        val record = DynamodbStreamRecordReplayJsonTest.streamRecord()
        val event = TableChangeEvent(id = "event-1", raw = DynamodbRaw(record))

        val snapshot = UnitOfWork(event = event).toSnapshot()

        val rawJson = snapshot.event.shouldNotBeNull().raw.shouldNotBeNull()
        Json.parseToJsonElement(rawJson).jsonObject["type"] shouldBe JsonPrimitive(RAW_DYNAMODB)
        Json.decodeFromString(RawRecord.serializer(), rawJson) shouldBe DynamodbRaw(record)
    }

    @Test
    fun `should leave record and raw absent when the unit of work has neither`() {
        val snapshot = UnitOfWork().toSnapshot()

        snapshot.record shouldBe null
        snapshot.event shouldBe null
    }
}
