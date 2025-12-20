package org.myorg.sut

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass


@Serializable
class TrackedUnitEvent() : Event {

    override var id: String? = null
    override var type: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = HashMap<String, String>()
    var entity: TrackedUnit? = null

    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null

    var location : String? = null
    var result : String? = null

    override fun toString(): String {
        return sutJson.encodeToString(this)
    }
}

val sutJson: Json = Json {
    ignoreUnknownKeys = true
    prettyPrint = true
    classDiscriminator = "mtype" // ensure your payload uses this key OR adjust to what your JSON contains
    serializersModule = SerializersModule {
        polymorphic(Event::class) {
            subclass(TrackedUnitEvent::class)
        }
    }
}

