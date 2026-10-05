package io.kopipes.core.serialization.snapshots

import io.kopipes.core.UnitOfWork

interface UnitOfWorkSnapshotter {
    fun snapshot(uow: UnitOfWork): UnitOfWorkSnapshot
}
