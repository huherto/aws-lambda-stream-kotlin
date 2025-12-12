package org.myorg.sut

import kotlinx.serialization.Serializable

@Serializable
class MyThing : Thing {
    override var id: String? = null
}

class MyEvent : Event {
    override var id: String? = null
    override var type: String? = null
    override var timestamp: Long? = 0
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = null
    override var entity: Thing? = null
    override var raw: Any? = null
    override var eem: Any? = null
}