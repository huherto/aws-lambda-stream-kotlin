package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent

interface Thing {
    var id: String?
}

interface Event {
    var id: String?
    var type: String?
    var timestamp: Long?
    var partitionKey: String?
    var tags: Map<String, String>?
    var entity: Thing?
    var raw: Any?
    var eem: Any?
}

class UnitOfWork<E : Event > {
    var record: KinesisEvent.KinesisEventRecord? = null
    var event : Event? = null
    var key : String? = null
    var sequenceNumber : String? = null
    var shardId : String? = null
    var timestamp : String? = null
}