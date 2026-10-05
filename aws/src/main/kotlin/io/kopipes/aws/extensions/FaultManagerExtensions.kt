package io.kopipes.aws.extensions

import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse
import io.kopipes.core.faults.FaultManager

fun FaultManager.kinesisRetryableFailures(): List<StreamsEventResponse.BatchItemFailure> {
    return pollRetryableItems().map { uow ->
        StreamsEventResponse.BatchItemFailure(uow.sequenceNumber)
    }
}
