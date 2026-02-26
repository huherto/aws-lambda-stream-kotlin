package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.Flow

interface EventsMicrostore {

    class SaveOptions(val expireDays: Int = 90) {}

    suspend fun save(flow: Flow<UnitOfWork>, options: SaveOptions)

}
