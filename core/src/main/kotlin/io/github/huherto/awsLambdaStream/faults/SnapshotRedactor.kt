package io.github.huherto.awsLambdaStream.faults

interface SnapshotRedactor {
    fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot
}

object NoOpSnapshotRedactor : SnapshotRedactor {
    override fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot = snapshot
}
