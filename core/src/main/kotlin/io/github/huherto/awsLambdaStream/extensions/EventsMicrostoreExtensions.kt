package io.github.huherto.awsLambdaStream.extensions

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.serialization.Snapshottable
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore

data class EventsMicrostoreExtensions(
    val queryParams: EventsMicrostore.QueryParams? = null,
    val saveOptions: EventsMicrostore.SaveOptions? = null,
) : Snapshottable {
    override fun toSnapshot(): Any {
        return mapOf(
            "queryParams" to queryParams?.toString(),
            "saveOptions" to saveOptions?.toString(),
        ).filterValues { it != null }
    }
}

val UnitOfWork.eventsMicrostore: EventsMicrostoreExtensions
    get() = getExtension() ?: EventsMicrostoreExtensions()

val UnitOfWork.queryParams: EventsMicrostore.QueryParams?
    get() = eventsMicrostore.queryParams

val UnitOfWork.saveOptions: EventsMicrostore.SaveOptions?
    get() = eventsMicrostore.saveOptions

fun UnitOfWork.withQueryParams(params: EventsMicrostore.QueryParams?): UnitOfWork =
    withExtension(eventsMicrostore.copy(queryParams = params))

fun UnitOfWork.withSaveOptions(options: EventsMicrostore.SaveOptions?): UnitOfWork =
    withExtension(eventsMicrostore.copy(saveOptions = options))
