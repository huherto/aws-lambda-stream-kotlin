package io.github.huherto.awsLambdaStream.serialization.snapshots

interface SnapshotRedactor {
    fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot
}

object NoOpSnapshotRedactor : SnapshotRedactor {
    override fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot = snapshot
}
