package io.github.huherto.`aws-lambda-stream`

import com.amazonaws.services.lambda.runtime.events.KinesisEvent

interface Event {
    var id: String?
    //var type: String?
    var timestamp: Long?
    var partitionKey: String?
    var tags: Map<String, String>?
    var raw: Any?
    var eem: Any?

    fun encoded()  : String

}

class UnitOfWork<E : Event > {
    var record: Any? = null
    var event : E? = null
    var key : String? = null

    var sequenceNumber : String? = null
    var shardId : String? = null
    var timestamp : String? = null
}