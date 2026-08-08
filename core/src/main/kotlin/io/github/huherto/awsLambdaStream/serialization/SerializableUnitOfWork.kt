package io.github.huherto.awsLambdaStream.serialization

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventReference
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.extensions.*
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

@Serializable
data class SerializableUnitOfWork(
    val pipeline: String? = null,
    val record: String? = null,
    val event: SerializableEvent? = null,
    val key: String? = null,
    val sequenceNumber: String? = null,
    val shardId: String? = null,
    val timestamp: String? = null,
    val meta: Map<String, String?>? = null,
    val triggers: List<EventReference>? = null,
    val correlated: List<EventReference>? = null,
    val batch: List<SerializableUnitOfWork>? = null,
    val batchGetRequest: String? = null,
    val batchGetResponse: String? = null,
    val publishRequest: String? = null,
    val publishRequestEntry: String? = null,
    val publishResponse: String? = null,
    val putRequest: String? = null,
    val putResponse: String? = null,
    val queryParams: String? = null,
    val queryRequest: String? = null,
    val queryResponse: String? = null,
    val saveOptions: String? = null,
    val scanRequest: String? = null,
    val updateRequest: String? = null,
    val updateResponse: String? = null,
    val s3: SerializableS3UnitOfWork? = null,
    val extensions: Map<String, String?> = emptyMap(),
) {
    constructor(unitOfWork: UnitOfWork) : this(
        pipeline = unitOfWork.pipeline?.toString(),
        record = unitOfWork.record?.toString(),
        event = unitOfWork.event?.let(::SerializableEvent),
        key = unitOfWork.key,
        sequenceNumber = unitOfWork.sequenceNumber,
        shardId = unitOfWork.shardId,
        timestamp = unitOfWork.timestamp,
        meta = unitOfWork.meta,
        triggers = unitOfWork.triggers?.map { it.toEventReference() },
        correlated = unitOfWork.correlated?.map { it.toEventReference() },
        batch = unitOfWork.batch?.map { SerializableUnitOfWork(it) },
        batchGetRequest = unitOfWork.batchGetRequest?.toString(),
        batchGetResponse = unitOfWork.batchGetResponse?.toString(),
        publishRequest = unitOfWork.publishRequest?.toString(),
        publishRequestEntry = unitOfWork.publishRequestEntry?.toString(),
        publishResponse = unitOfWork.publishResponse?.toString(),
        putRequest = unitOfWork.putRequest?.toString(),
        putResponse = unitOfWork.putResponse?.toString(),
        queryParams = unitOfWork.queryParams?.toString(),
        queryRequest = unitOfWork.queryRequest?.toString(),
        queryResponse = unitOfWork.queryResponse?.toString(),
        saveOptions = unitOfWork.saveOptions?.toString(),
        scanRequest = unitOfWork.scanRequest?.toString(),
        updateRequest = unitOfWork.updateRequest?.toString(),
        updateResponse = unitOfWork.updateResponse?.toString(),
        s3 = unitOfWork.s3.takeUnless { it.isEmpty() }?.let(::SerializableS3UnitOfWork),
        extensions = unitOfWork.extensions.entries.associate { (k, v) ->
            val name = k.simpleName ?: "unknown"
            val snapshot = if (v is Snapshottable) v.toSnapshot() else v
            name to snapshot?.toString()
        },
    )
}

@Serializable
data class SerializableEvent(
    val id: String? = null,
    val type: String? = null,
    val timestamp: Long? = null,
    val partitionKey: String? = null,
    val tags: Map<String, String>? = null,
    val raw: String? = null,
    val eem: String? = null,
    val triggers: List<EventReference>? = null,
    val encoded: String? = null,
) {
    constructor(event: Event) : this(
        id = event.id,
        type = event.eventType(),
        timestamp = event.timestamp,
        partitionKey = event.partitionKey,
        tags = event.tags,
        raw = event.raw?.toString(),
        eem = event.eem?.toString(),
        triggers = event.triggers,
        encoded = event.toString(),
    )
}

@Serializable
data class SerializableS3UnitOfWork(
    val getRequest: String? = null,
    val getResponse: String? = null,
    val deleteRequest: String? = null,
    val deleteResponse: String? = null,
    val copyRequest: String? = null,
    val copyResponse: String? = null,
    val getResponseText: String? = null,
    val getResponseBytes: ByteArray? = null,
    val putRequest: String? = null,
    val putResponse: String? = null,
    val listRequest: String? = null,
    val listResponse: String? = null,
    val listResponseObject: String? = null,
    val headRequest: String? = null,
    val headResponse: String? = null,
) {
    constructor(s3: S3UnitOfWork) : this(
        getRequest = s3.getRequest?.toString(),
        getResponse = s3.getResponse?.toString(),
        deleteRequest = s3.deleteRequest?.toString(),
        deleteResponse = s3.deleteResponse?.toString(),
        copyRequest = s3.copyRequest?.toString(),
        copyResponse = s3.copyResponse?.toString(),
        getResponseText = s3.getResponseText,
        getResponseBytes = s3.getResponseBytes,
        putRequest = s3.putRequest?.toString(),
        putResponse = s3.putResponse?.toString(),
        listRequest = s3.listRequest?.toString(),
        listResponse = s3.listResponse?.toString(),
        listResponseObject = s3.listResponseObject?.toString(),
        headRequest = s3.headRequest?.toString(),
        headResponse = s3.headResponse?.toString(),
    )
}

fun UnitOfWork.toSerializableUnitOfWork(): SerializableUnitOfWork =
    SerializableUnitOfWork(this)

private fun Event.toEventReference(): EventReference =
    EventReference(
        id = id,
        type = eventType(),
        timestamp = timestamp,
    )

object UnitOfWorkAsSerializableUnitOfWorkSerializer : KSerializer<UnitOfWork?> {
    private val surrogateSerializer = SerializableUnitOfWork.serializer().nullable

    override val descriptor: SerialDescriptor =
        surrogateSerializer.descriptor

    override fun serialize(encoder: Encoder, value: UnitOfWork?) {
        encoder.encodeSerializableValue(
            surrogateSerializer,
            value?.toSerializableUnitOfWork(),
        )
    }

    override fun deserialize(decoder: Decoder): UnitOfWork? {
        throw SerializationException(
            "UnitOfWork deserialization is not supported. SerializableUnitOfWork is a one-way serialization surrogate.",
        )
    }
}