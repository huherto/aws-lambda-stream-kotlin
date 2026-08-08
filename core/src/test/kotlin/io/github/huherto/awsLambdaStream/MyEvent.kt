package io.github.huherto.awsLambdaStream

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializerOrNull

@Serializable
data class MyThing(val id: String? = null)

@Serializable
sealed class MyEvent : BaseEvent() {
    override val id: String? get() = null
    override val timestamp: Long? get() = null
    override val partitionKey: String? get() = null
    override val tags: Map<String, String>? get() = null
    @kotlinx.serialization.Contextual
    override val raw: Any? get() = null
    @kotlinx.serialization.Contextual
    override val eem: Any? get() = null
    override val triggers: List<EventReference>? get() = null
    abstract val entity: MyThing?

    @OptIn(InternalSerializationApi::class)
    override fun eventType(): String {
        return this::class.serializerOrNull()?.descriptor?.serialName ?: "unknown"
    }

    @Deprecated(
        message = "Use EventCodec or the configured framework publisher instead.",
    )
    override fun encoded(): String {
        return sutJson.encodeToString(serializer(), this)
    }
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
    @kotlinx.serialization.Contextual
    override val raw: Any? = null,
    @kotlinx.serialization.Contextual
    override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: MyThing? = null
) : MyEvent()

@SerialName("MY_EVENT_B")
@Serializable
data class MyEventB(
    val foo: String? = null,
    val bar: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    @kotlinx.serialization.Contextual
    override val raw: Any? = null,
    @kotlinx.serialization.Contextual
    override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: MyThing? = null
) : MyEvent()

@SerialName("MY_EVENT_C")
@Serializable
data class MyEventC(
    val foo: String? = null,
    val bar: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    @kotlinx.serialization.Contextual
    override val raw: Any? = null,
    @kotlinx.serialization.Contextual
    override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: MyThing? = null
) : MyEvent()

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
    prettyPrint = true
    isLenient = true
    classDiscriminator = "type"
}