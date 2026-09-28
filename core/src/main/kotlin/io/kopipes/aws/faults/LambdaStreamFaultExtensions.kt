package io.kopipes.aws.faults

import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse

/**
 * Extension for extracting Kinesis/DynamoDB Streams retryable batch item failures from [FaultManager].
 */
fun FaultManager.kinesisRetryableFailures(): List<StreamsEventResponse.BatchItemFailure> {
    return pollRetryableItems().map { uow ->
        StreamsEventResponse.BatchItemFailure(uow.sequenceNumber)
    }
}
