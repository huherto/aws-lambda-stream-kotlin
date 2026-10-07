package io.kopipes.aws.from

import io.kopipes.aws.ImagesRaw
import io.kopipes.aws.serialization.RecordImageSerializer
import io.kopipes.aws.utils.StreamAttributeValueMapReader
import io.kopipes.core.utils.AttributeValueMapReader
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json.Default.decodeFromString
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

/** The before/after images of a table change. */
interface RecordPair {
    val new: RecordImage?
    val old: RecordImage?
}

fun RecordPair(new: RecordImage?, old: RecordImage?): ImagesRaw = ImagesRaw(new, old)

/** Represents a record image in a DynamoDB stream. */
@Serializable(with = RecordImageSerializer::class)
class RecordImage(
    val map: Map<String, EventAV?>,
) : Map<String, EventAV?> by map, AttributeValueMapReader by StreamAttributeValueMapReader(map) {

    fun getPk(): String? = getS("pk")

    fun getTtl(): String? = map["ttl"]?.n

    fun getData(): String? = getS("data")

    fun getEvent(): String? = getS("event")

    fun getDiscriminator(): String? = getS("discriminator")

    fun getSuffix(): String? = getS("suffix")

    fun isDeleted(): Boolean = getBool("deleted") == true

    fun latched(): Boolean = getBool("latched") == true

    // TODO: Not sure if this is the best way to do this. It adds a dependency on kotlinx.serialization.
    inline fun <reified T> getDecodedObject(fieldName: String): T? {
        return getS(fieldName)?.let {
            decodeFromString<T>(it)
        }
    }

    override fun equals(other: Any?): Boolean = this === other || (other is RecordImage && map == other.map)

    override fun hashCode(): Int = map.hashCode()

    override fun toString(): String = map.toString()
}
