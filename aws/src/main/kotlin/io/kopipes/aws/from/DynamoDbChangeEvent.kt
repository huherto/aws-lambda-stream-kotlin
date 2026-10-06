package io.kopipes.aws.from

import io.kopipes.core.EnvelopeEncryptionMetadata
import io.kopipes.core.EventReference
import io.kopipes.core.RawRecord
import io.kopipes.core.TableChangeEvent
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * DynamoDB implementation of [TableChangeEvent].
 * Encapsulates [RecordPair] and [RecordImage] access.
 */
@Serializable
data class DynamoDbChangeEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val type: String? = null,
) : TableChangeEvent() {

    val recordPair: RecordPair?
        get() = raw as? RecordPair

    val newImage: RecordImage?
        get() = recordPair?.new

    val oldImage: RecordImage?
        get() = recordPair?.old

    override fun getPk(): String? = newImage?.getPk()
    override fun getTtl(): String? = newImage?.getTtl()
    override fun getData(): String? = newImage?.getData()
    override fun getEvent(): String? = newImage?.getEvent()
    override fun getDiscriminator(): String? = newImage?.getDiscriminator()
    override fun getSuffix(): String? = newImage?.getSuffix()
    override fun isDeleted(): Boolean = newImage?.isDeleted() == true
    override fun latched(): Boolean = newImage?.latched() == true
    override fun getS(fieldName: String): String? = newImage?.getS(fieldName)
    override fun getDouble(fieldName: String): Double? = newImage?.getDouble(fieldName)
    override fun getLong(fieldName: String): Long? = newImage?.getLong(fieldName)

    override fun toString(): String {
        return Json.encodeToString(serializer(), this)
    }

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): DynamoDbChangeEvent = copy(
        id = id,
        timestamp = timestamp,
        partitionKey = partitionKey,
        tags = tags,
        raw = raw,
        eem = eem,
        triggers = triggers
    )
}
