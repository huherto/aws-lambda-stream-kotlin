package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow

interface EventsMicrostore {

    data class SaveOptions(
        val pk: String,
        val sk: String,
        val discriminator : String,
        val timeStamp : Long?,
        val awsRegion : String? = null,
        val sequenceNumber : String? = null,
        val ttl: Long? = null, // Epoch in seconds.
        val expire: Boolean,
        val data : String? = null,
        val includeRaw: Boolean = true,
        val suffix : String,
        val pipelineId : String? = null,
    )

    data class QueryParams(
        val pk: String? = null,
        val correlation: Boolean,
        val data: String? = null,
        val index: String? = null,
    )

    fun save(flow: Flow<UnitOfWork>) : Flow<UnitOfWork>

    fun queryByPk(flow: Flow<UnitOfWork>): Flow<UnitOfWork>

}