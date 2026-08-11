package io.github.huherto.awsLambdaStream.faults

import io.github.huherto.awsLambdaStream.FAULT_EVENT_TYPE
import io.github.huherto.awsLambdaStream.serialization.snapshots.ErrorSnapshot
import io.github.huherto.awsLambdaStream.serialization.snapshots.UnitOfWorkSnapshot
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable

@Serializable
data class FaultEvent(
    val id: String? = null,
    @OptIn(ExperimentalSerializationApi::class)
    @EncodeDefault
    val type: String = FAULT_EVENT_TYPE,
    val timestamp: Long? = null,
    val partitionKey: String? = null,
    val tags: Map<String, String>? = null,
    val err: ErrorSnapshot? = null,
    val uow: UnitOfWorkSnapshot? = null,

    @kotlinx.serialization.Transient
    @com.fasterxml.jackson.annotation.JsonIgnore
    val runtimeUow: io.github.huherto.awsLambdaStream.UnitOfWork? = null,

    @kotlinx.serialization.Transient
    @com.fasterxml.jackson.annotation.JsonIgnore
    val faultException: io.github.huherto.awsLambdaStream.FaultException? = null
) {
    override fun toString(): String {
        return err?.message ?: "Unknown Error"
    }
}
