package org.myorg.sut

import io.github.huherto.`aws-lambda-stream`.Event
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic

@Serializable
class MyThing {
    var id: String? = null
}

val myJson: Json = Json {
    ignoreUnknownKeys = true
    prettyPrint = true
    classDiscriminator = "mtype" // ensure your payload uses this key OR adjust to what your JSON contains
    serializersModule = SerializersModule {
        polymorphic(Event::class) {
        }
    }
    fun decoded(s: String): MyEvent {
        return myJson.decodeFromString<MyEvent>(s)
    }
}

@Serializable
class MyEvent : Event {
    override var id: String? = null
    override var type: String? = null
    override var timestamp: Long? = 0
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = null
    var entity: MyThing? = null

    @Contextual
    override var raw: Any? = null
    @Contextual
    override var eem: Any? = null

    override fun encoded(): String {
        return myJson.encodeToString(this)
    }


}