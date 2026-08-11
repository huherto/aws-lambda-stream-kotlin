package io.github.huherto.awsLambdaStream.serialization.snapshots

import io.github.huherto.awsLambdaStream.UnitOfWork

interface UnitOfWorkSnapshotter {
    fun snapshot(uow: UnitOfWork): UnitOfWorkSnapshot
}
