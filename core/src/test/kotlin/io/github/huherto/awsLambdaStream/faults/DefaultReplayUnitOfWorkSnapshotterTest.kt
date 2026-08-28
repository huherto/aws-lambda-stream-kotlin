package io.github.huherto.awsLambdaStream.faults

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.flow.Flow
import org.junit.jupiter.api.Test

class DefaultReplayUnitOfWorkSnapshotterTest {

    private val snapshotter = DefaultUnitOfWorkSnapshotter()

    @Test
    fun `snapshot should capture basic UnitOfWork fields`() {
        // Arrange
        val uow = UnitOfWork(
            pipeline = object : Pipeline("pipe-1") {
                override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) = fromFlow
            },
            key = "key-1",
            sequenceNumber = "seq-1",
            timestamp = "2023-01-01T00:00:00Z"
        )

        // Act
        val snapshot = snapshotter.snapshot(uow)

        // Assert
        snapshot.pipeline?.id shouldBe "pipe-1"
        snapshot.key shouldBe "key-1"
        snapshot.sequenceNumber shouldBe "seq-1"
        snapshot.timestamp shouldBe "2023-01-01T00:00:00Z"
    }

    @Test
    fun `snapshot should capture batch recursively`() {
        // Arrange
        val childUow = UnitOfWork(key = "child-key")
        val parentUow = UnitOfWork(key = "parent-key", batch = listOf(childUow))

        // Act
        val snapshot = snapshotter.snapshot(parentUow)

        // Assert
        snapshot.key shouldBe "parent-key"
        snapshot.batch shouldNotBe null
        snapshot.batch!!.size shouldBe 1
        snapshot.batch!![0].key shouldBe "child-key"
    }
}
