package io.github.huherto.awsLambdaStream.extensions

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.ConnectorResponse
import io.github.huherto.awsLambdaStream.serialization.Snapshottable

data class EventBridgeExtensions(
    val publishRequest: PutEventsRequest? = null,
    val publishRequestEntry: PutEventsRequestEntry? = null,
    val publishResponse: ConnectorResponse? = null,
) : Snapshottable {
    override fun toSnapshot(): Any {
        return mapOf(
            "publishRequest" to publishRequest?.toString(),
            "publishRequestEntry" to publishRequestEntry?.toString(),
            "publishResponse" to publishResponse?.toString(),
        ).filterValues { it != null }
    }
}

val UnitOfWork.eventBridge: EventBridgeExtensions
    get() = getExtension() ?: EventBridgeExtensions()

val UnitOfWork.publishRequest: PutEventsRequest?
    get() = eventBridge.publishRequest

val UnitOfWork.publishRequestEntry: PutEventsRequestEntry?
    get() = eventBridge.publishRequestEntry

val UnitOfWork.publishResponse: ConnectorResponse?
    get() = eventBridge.publishResponse

fun UnitOfWork.withPublishRequest(request: PutEventsRequest?): UnitOfWork =
    withExtension(eventBridge.copy(publishRequest = request))

fun UnitOfWork.withPublishRequestEntry(entry: PutEventsRequestEntry?): UnitOfWork =
    withExtension(eventBridge.copy(publishRequestEntry = entry))

fun UnitOfWork.withPublishResponse(response: ConnectorResponse?): UnitOfWork =
    withExtension(eventBridge.copy(publishResponse = response))
