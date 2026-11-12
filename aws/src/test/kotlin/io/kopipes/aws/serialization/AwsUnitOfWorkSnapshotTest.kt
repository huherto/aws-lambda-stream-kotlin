package io.kopipes.aws.serialization

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.captureAwsOperations
import io.kopipes.aws.DynamodbRaw
import io.kopipes.aws.from.DynamoDbChangeEvent
import io.kopipes.aws.serialization.aws.DynamodbSerializationTest
import io.kopipes.aws.serialization.aws.DynamodbStreamRecordReplayJson
import io.kopipes.core.RAW_DYNAMODB
import io.kopipes.core.RawRecord
import io.kopipes.core.UnitOfWork
import io.kopipes.core.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.Test

/** Tests for UnitOfWork snapshotting with AWS types. */
class AwsUnitOfWorkSnapshotTest {

    private val snapshotter = DefaultUnitOfWorkSnapshotter(
        recordSnapshotters = AwsKopipesContext.defaultRecordSnapshotters(),
        awsOperationCapturer = ::captureAwsOperations,
    )

    @Test
    fun `should encode the source record as replayable json`() {
        val record = DynamodbSerializationTest.streamRecord()

        val snapshot = snapshotter.snapshot(UnitOfWork(record = record))

        val recordSnapshot = snapshot.record.shouldNotBeNull()
        recordSnapshot.kind shouldBe "dynamodb"
        val recordJson = Json.encodeToString(recordSnapshot.payload)
        DynamodbStreamRecordReplayJson.decode(recordJson) shouldBe record
    }

    @Test
    fun `should encode event raw as a discriminated raw record`() {
        val record = DynamodbSerializationTest.streamRecord()
        val event = DynamoDbChangeEvent(id = "event-1", raw = DynamodbRaw(record))

        val snapshot = snapshotter.snapshot(UnitOfWork(event = event))

        val rawJson = snapshot.event.shouldNotBeNull().raw.shouldNotBeNull()
        Json.parseToJsonElement(rawJson).jsonObject["type"] shouldBe JsonPrimitive(RAW_DYNAMODB)
        Json.decodeFromString(RawRecord.serializer(), rawJson) shouldBe DynamodbRaw(record)
    }

    @Test
    fun `should leave record and raw absent when the unit of work has neither`() {
        val snapshot = snapshotter.snapshot(UnitOfWork())

        snapshot.record shouldBe null
        snapshot.event shouldBe null
    }
}
