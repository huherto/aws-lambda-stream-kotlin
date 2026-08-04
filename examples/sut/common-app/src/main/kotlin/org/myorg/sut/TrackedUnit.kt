package org.myorg.sut

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
class TrackedUnit {

    var id: String? = null

    var senderFullName : String? = null
    var returnAddress : Address? = null
    var destinationAddress : Address? = null
    var trackingNumber : String? = null
    var weight : Double? = null
    var dimensions: PackageDimensions? = null

    @Serializable
    class Address(val street : String, val city : String, val state : String, val zip : String) {
        fun toJson() : String {
            return Json.encodeToString(this)
        }
    }

    @Serializable
    class PackageDimensions(val length : Double, val width : Double, val height : Double)

    fun toJson() : String {
        return Json.encodeToString(this)
    }

    override fun toString() : String = toJson()
}