package io.kopipes.aws.serialization.snapshots

interface SnapshotRedactor {
    fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot
}

object NoOpSnapshotRedactor : SnapshotRedactor {
    override fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot = snapshot
}
