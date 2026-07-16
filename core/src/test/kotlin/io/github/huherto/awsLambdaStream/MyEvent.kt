package io.github.huherto.awsLambdaStream

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import kotlinx.serialization.serializerOrNull

@Serializable
class MyThing {
    var id: String? = null
}

@Serializable
sealed class MyEvent : BaseEvent() {

    var entity: MyThing? = null

    @OptIn(InternalSerializationApi::class)
    override fun eventType(): String {
        return this::class.serializerOrNull()?.descriptor?.serialName ?: "unknown"
    }

    override fun encoded(): String {
        return sutJson.encodeToString(serializer(), this)
    }
}

@Serializable
@SerialName("MY_EVENT_A")
data class MyEventA(var foo: String? = null, var bar: String? = null) : MyEvent()

@Serializable
@SerialName("MY_EVENT_B")
data class MyEventB(var foo: String? = null, var bar: String? = null) : MyEvent()

@Serializable
@SerialName("MY_EVENT_C")
data class MyEventC(var foo: String? = null, var bar: String? = null) : MyEvent()

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
    serializersModule = SerializersModule {
        polymorphic(MyEvent::class) {
            subclass(MyEventA::class)
            subclass(MyEventB::class)
            subclass(MyEventC::class)
        }
    }
}