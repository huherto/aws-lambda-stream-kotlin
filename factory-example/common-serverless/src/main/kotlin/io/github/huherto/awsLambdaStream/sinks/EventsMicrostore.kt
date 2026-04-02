package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow

interface EventsMicrostore {

    data class SaveOptions(
        val includeRaw: Boolean = false,
        val ttlTimestampInSecs: Long? = null,
        val expire: String? = null,
    )

    fun save(flow: Flow<UnitOfWork>) : Flow<UnitOfWork>

}