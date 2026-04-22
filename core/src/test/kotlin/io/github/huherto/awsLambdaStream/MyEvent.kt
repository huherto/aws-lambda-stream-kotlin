package io.github.huherto.awsLambdaStream

import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass

@Serializable
class MyThing {
    var id: String? = null
}

@Serializable
sealed class MyEvent() : BaseEvent() {

    var entity: MyThing? = null

    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null

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