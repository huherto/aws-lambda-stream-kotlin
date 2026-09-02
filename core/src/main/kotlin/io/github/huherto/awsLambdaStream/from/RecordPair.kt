package io.github.huherto.awsLambdaStream.from

import io.github.huherto.awsLambdaStream.ImagesRaw
import io.github.huherto.awsLambdaStream.serialization.RecordImageSerializer
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
class RecordImage(val map: Map<String, EventAV?>) : Map<String, EventAV?> by map {

    fun getPk(): String? = map["pk"]?.s

    fun getTtl(): String? = map["ttl"]?.n

    fun getData(): String? = map["data"]?.s

    fun getEvent(): String? = map["event"]?.s

    fun getDiscriminator(): String? = map["discriminator"]?.s

    fun getSuffix(): String? = map["suffix"]?.s

    fun isDeleted(): Boolean = map["deleted"]?.bool == true

    fun latched(): Boolean = map["latched"]?.bool == true

    fun getS(fieldName: String): String? = map[fieldName]?.s

    fun getDouble(fieldName: String): Double? = map[fieldName]?.n?.toDouble()

    fun getLong(fieldName: String): Long? = map[fieldName]?.n?.toLong()

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
