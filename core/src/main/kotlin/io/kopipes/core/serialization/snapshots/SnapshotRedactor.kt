package io.kopipes.core.serialization.snapshots

interface SnapshotRedactor {
    fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot
}

object NoOpSnapshotRedactor : SnapshotRedactor {
    override fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot = snapshot
}
