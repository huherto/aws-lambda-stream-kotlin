package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow

interface EventsMicrostore {

    data class SaveOptions(
        val pk: String? = null,
        val sk: String? = null,
        val discriminator : String? = null,
        val timeStamp : String? = null,
        val awsRegion : String? = null,
        val sequenceNumber : String? = null,
        val ttl: Long? = null, // Epoch in seconds.
        val expire: Boolean? = null,
        val data : String? = null,
        val includeRaw: Boolean = true,
        val suffix : String? = null,
        val pipelineId : String? = null,
    )

    data class QueryParams(
        val pk: String? = null,
        val isCorrelated: Boolean,
        val data: String? = null,
        val index: String? = null,
    )

    fun save(flow: Flow<UnitOfWork>) : Flow<UnitOfWork>

    fun queryByPk(flow: Flow<UnitOfWork>): Flow<UnitOfWork>

}