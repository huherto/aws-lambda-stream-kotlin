package org.myorg.sut

import kotlinx.serialization.Serializable

@Serializable
class TrackedUnit : Thing {

    override var id: String? = null
    override var timestamp: String? = null

}