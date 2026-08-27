package io.github.huherto.awsLambdaStream

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializerOrNull

@Serializable
data class MyThing(val id: String? = null)

@Serializable
sealed class MyEvent : Event {
    override val id: String? get() = null
    override val timestamp: Long? get() = null
    override val partitionKey: String? get() = null
    override val tags: Map<String, String>? get() = null
    override val raw: RawRecord? get() = null
    override val eem: EnvelopeEncryptionMetadata? get() = null
    override val triggers: List<EventReference>? get() = null
    abstract val entity: MyThing?

    @OptIn(InternalSerializationApi::class)
    override fun eventType(): String {
        return this::class.serializerOrNull()?.descriptor?.serialName ?: "unknown"
    }

    override fun toString(): String {
        return sutJson.encodeToString(MyEvent.serializer(), this)
    }

    abstract override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): MyEvent
}

@SerialName("MY_EVENT_A")
@Serializable
data class MyEventA(
    val foo: String? = null,
    val bar: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: MyThing? = null
) : MyEvent() {
    override fun toString() = super.toString()

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): MyEvent = copy(
        id = id,
        timestamp = timestamp,
        partitionKey = partitionKey,
        tags = tags,
        raw = raw,
        eem = eem,
        triggers = triggers
    )
}

@SerialName("MY_EVENT_B")
@Serializable
data class MyEventB(
    val foo: String? = null,
    val bar: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: MyThing? = null
) : MyEvent() {
    override fun toString() = super.toString()

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): MyEvent = copy(
        id = id,
        timestamp = timestamp,
        partitionKey = partitionKey,
        tags = tags,
        raw = raw,
        eem = eem,
        triggers = triggers
    )
}

@SerialName("MY_EVENT_C")
@Serializable
data class MyEventC(
    val foo: String? = null,
    val bar: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: MyThing? = null
) : MyEvent() {
    override fun toString() = super.toString()

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): MyEvent = copy(
        id = id,
        timestamp = timestamp,
        partitionKey = partitionKey,
        tags = tags,
        raw = raw,
        eem = eem,
        triggers = triggers
    )
}

class MyEventCodec : EventCodec {

    override fun decode(eventAsString: String): Event {
        return sutJson.decodeFromString(MyEvent.serializer(), eventAsString)
    }

    override fun encode(event: Event): String {
        require(event is MyEvent) {
            "MyEventCodec can only encode MyEvent instances, but received ${event::class.qualifiedName}"
        }

        return sutJson.encodeToString(MyEvent.serializer(), event)
    }
}

val sutJson: Json = Json {
    ignoreUnknownKeys = true
    prettyPrint = false
    isLenient = true
    classDiscriminator = "type"
}