package io.github.huherto.`aws-lambda-stream`

import kotlinx.coroutines.flow.Flow

interface EventsMicrostore<E : Event > {

    class SaveOptions(val expireDays: Int = 90) {}

    suspend fun save(flow: Flow<UnitOfWork<E>>, options: SaveOptions)

}
