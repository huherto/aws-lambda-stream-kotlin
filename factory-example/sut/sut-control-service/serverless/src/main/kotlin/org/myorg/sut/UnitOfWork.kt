package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent

interface Thing {
    var id: String?
    val timestamp: String?
}

class UnitOfWork<T : Thing > {
    var record: KinesisEvent.KinesisEventRecord? = null
    var event : T? = null
    var key : String? = null
    var sequenceNumber : String? = null
    var shardId : String? = null
    var timestamp : String? = null
}