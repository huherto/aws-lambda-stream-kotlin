package io.github.huherto.awsLambdaStream.extensions

import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataRequest
import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataResponse
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.serialization.Snapshottable

data class CloudWatchExtensions(
    val putMetricDataRequest: PutMetricDataRequest? = null,
    val putMetricDataResponse: PutMetricDataResponse? = null,
) : Snapshottable {
    override fun toSnapshot(): Any {
        return mapOf(
            "putMetricDataRequest" to putMetricDataRequest?.toString(),
            "putMetricDataResponse" to putMetricDataResponse?.toString(),
        ).filterValues { it != null }
    }
}

val UnitOfWork.cloudWatch: CloudWatchExtensions
    get() = getExtension() ?: CloudWatchExtensions()

val UnitOfWork.putMetricDataRequest: PutMetricDataRequest?
    get() = cloudWatch.putMetricDataRequest

val UnitOfWork.putMetricDataResponse: PutMetricDataResponse?
    get() = cloudWatch.putMetricDataResponse

fun UnitOfWork.withPutMetricDataRequest(request: PutMetricDataRequest?): UnitOfWork =
    withExtension(cloudWatch.copy(putMetricDataRequest = request))

fun UnitOfWork.withPutMetricDataResponse(response: PutMetricDataResponse?): UnitOfWork =
    withExtension(cloudWatch.copy(putMetricDataResponse = response))

fun UnitOfWork.copyCloudWatch(
    transform: CloudWatchExtensions.() -> CloudWatchExtensions,
): UnitOfWork {
    val updated = cloudWatch.transform()
    return withExtension(updated)
}
