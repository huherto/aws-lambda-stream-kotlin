package io.github.huherto.awsLambdaStream.faults

import io.github.huherto.awsLambdaStream.UnitOfWork

interface UnitOfWorkSnapshotter {
    fun snapshot(uow: UnitOfWork): UnitOfWorkSnapshot
}
