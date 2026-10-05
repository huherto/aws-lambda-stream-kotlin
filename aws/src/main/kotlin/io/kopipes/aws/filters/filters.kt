package io.kopipes.aws.filters

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.aws.from.RecordPair
import io.kopipes.core.UnitOfWork

fun outLatched(uow: UnitOfWork): Boolean {
    val raw = uow.event?.raw as? RecordPair
    if (raw?.new?.latched() == true) return false
    val record = uow.record as? DynamodbEvent.DynamodbStreamRecord
    return record?.dynamodb?.newImage?.get("latched")?.bool != true
}

fun outSourceIsSelf(uow: UnitOfWork): Boolean {
    val record = uow.record as? DynamodbEvent.DynamodbStreamRecord
    val pipelineId = uow.pipeline?.id
    val recordPipelineId = record?.dynamodb?.newImage?.get("pipelineId")?.s
    return pipelineId == null || recordPipelineId == null || pipelineId != recordPipelineId
}
