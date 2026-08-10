package io.github.huherto.awsLambdaStream.from

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.github.huherto.awsLambdaStream.DynamodbRaw
import io.github.huherto.awsLambdaStream.ImagesRaw
import io.github.huherto.awsLambdaStream.serialization.RecordImageJacksonDeserializer
import io.github.huherto.awsLambdaStream.serialization.RecordImageJacksonSerializer
import io.github.huherto.awsLambdaStream.serialization.RecordImageSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json.Default.decodeFromString
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

/**
 * The before/after images of a table change.
 *
 * This is a read-only view rather than a container: [DynamodbRaw] derives it from the original
 * stream record, and [ImagesRaw] holds images directly for events that were synthesized without
 * one.
 */
interface RecordPair {
    val new: RecordImage?
    val old: RecordImage?
}

/**
 * Creates a [RecordPair] from images alone. Prefer [DynamodbRaw] whenever the originating stream
 * record is available, since it keeps the record intact for replay.
 */
fun RecordPair(new: RecordImage?, old: RecordImage?): ImagesRaw = ImagesRaw(new, old)

@Serializable(with = RecordImageSerializer::class)
@JsonSerialize(using = RecordImageJacksonSerializer::class)
@JsonDeserialize(using = RecordImageJacksonDeserializer::class)
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
