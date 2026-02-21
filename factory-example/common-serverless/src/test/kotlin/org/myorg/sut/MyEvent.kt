package org.myorg.sut

import io.github.huherto.`aws-lambda-stream`.Event
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass

@Serializable
class MyThing {
    var id: String? = null
}

@Serializable
sealed class MyEvent(@Transient override var type: String = "") : Event {
    override var id: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = mutableMapOf()

    var entity: MyThing? = null

    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null

    override fun encoded(): String {
        return sutJson.encodeToString(MyEvent.serializer(), this)
    }
}

@Serializable
@SerialName("MY_EVENT_A")
data class MyEventA(var foo: String? = null, var bar: String? = null) : MyEvent("MY_EVENT_A")

@Serializable
@SerialName("MY_EVENT_B")
data class MyEventB(var foo: String? = null, var bar: String? = null) : MyEvent("MY_EVENT_B")

@Serializable
@SerialName("MY_EVENT_C")
data class MyEventC(var foo: String? = null, var bar: String? = null) : MyEvent("MY_EVENT_C")

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