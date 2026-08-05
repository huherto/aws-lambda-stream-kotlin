package io.github.huherto.awsLambdaStream.faults

import io.github.huherto.awsLambdaStream.FAULT_EVENT_TYPE
import io.github.huherto.awsLambdaStream.FaultException
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable

@Serializable
class FaultEvent {
    var id: String? = null
    @OptIn(ExperimentalSerializationApi::class)
    @EncodeDefault
    var type: String = FAULT_EVENT_TYPE
    var timestamp: Long? = null
    var partitionKey: String? = null
    var tags: Map<String, String>? = null
    var err: ErrorSnapshot? = null
    var uow: UnitOfWorkSnapshot? = null

    @kotlinx.serialization.Transient
    @com.fasterxml.jackson.annotation.JsonIgnore
    var runtimeUow: UnitOfWork? = null

    @kotlinx.serialization.Transient
    @com.fasterxml.jackson.annotation.JsonIgnore
    var faultException: FaultException? = null

    override fun toString(): String {
        return err?.message ?: "Unknown Error"
    }
}
