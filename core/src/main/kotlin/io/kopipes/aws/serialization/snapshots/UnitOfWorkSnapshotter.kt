package io.kopipes.aws.serialization.snapshots

import io.kopipes.aws.UnitOfWork

interface UnitOfWorkSnapshotter {
    fun snapshot(uow: UnitOfWork): UnitOfWorkSnapshot
}
