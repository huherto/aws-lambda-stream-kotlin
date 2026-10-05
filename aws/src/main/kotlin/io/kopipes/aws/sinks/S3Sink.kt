package io.kopipes.aws.sinks

import io.kopipes.aws.awsEnvConfig
import io.kopipes.aws.connectors.S3Connector
import io.kopipes.aws.extensions.copyS3
import io.kopipes.aws.extensions.s3
import io.kopipes.core.UnitOfWork
import io.kopipes.core.metrics.withStepMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

class S3Sink(
    private val s3ConnectorOptions: S3Connector.Options = S3Connector.Options(),
    private val bucketName: String? = null,
) {

    private fun getConnector(): S3Connector {
        return S3Connector(s3ConnectorOptions)
    }

    private fun resolveBucketName(): String {
        return bucketName ?: awsEnvConfig().bucketName() ?: error("bucketName is not set")
    }

    fun Flow<UnitOfWork>.rateLimit(): Flow<UnitOfWork> = this

    fun ensurePutRequestBucket(uow: UnitOfWork): UnitOfWork {
        val s3 = uow.s3
        val putRequest = s3.putRequest
        if (putRequest != null) {
            if (putRequest.bucket == null) {
                return uow.copyS3 { copy(putRequest = putRequest.copy { bucket = resolveBucketName() }) }
            }
        }
        return uow
    }

    fun ensureDeleteRequestBucket(uow: UnitOfWork): UnitOfWork {
        val s3 = uow.s3
        val deleteRequest = s3.deleteRequest
        if (deleteRequest != null) {
            if (deleteRequest.bucket == null) {
                return uow.copyS3 { copy(deleteRequest = deleteRequest.copy { bucket = resolveBucketName() }) }
            }
        }
        return uow
    }

    fun ensureCopyRequestBucket(uow: UnitOfWork): UnitOfWork {
        val s3 = uow.s3
        val copyRequest = s3.copyRequest
        if (copyRequest != null) {
            if (copyRequest.bucket == null) {
                return uow.copyS3 { copy(copyRequest = copyRequest.copy { bucket = resolveBucketName() }) }
            }
        }
        return uow
    }

    fun putObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.rateLimit()
            .map { uow -> ensurePutRequestBucket(uow) }
            .map { uow ->
                val putRequest = uow.s3.putRequest ?: return@map uow
                uow.withStepMetrics("save") { uowWithMetrics ->
                    val response = getConnector().putObject(putRequest, uowWithMetrics)
                    uowWithMetrics.copyS3 {
                        copy(putResponse = response)
                    }
                }
            }
    }

    fun deleteObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.rateLimit()
            .map { uow -> ensureDeleteRequestBucket(uow) }
            .map { uow ->
                val deleteRequest = uow.s3.deleteRequest ?: return@map uow
                uow.withStepMetrics("delete") { uowWithMetrics ->
                    val response = getConnector().deleteObject(uowWithMetrics.s3.deleteRequest!!, uowWithMetrics)
                    uowWithMetrics.copyS3 {
                        copy(deleteResponse = response)
                    }
                }
            }
    }

    fun copyObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.rateLimit()
            .map { uow -> ensureCopyRequestBucket(uow) }
            .map { uow ->
                val request = uow.s3.copyRequest ?: return@map uow
                uow.withStepMetrics("copy") { uowWithMetrics ->
                    val response = getConnector().copyObject(uowWithMetrics.s3.copyRequest!!, uowWithMetrics)
                    uowWithMetrics.copyS3 {
                        copy(copyResponse = response)
                    }
                }
            }
    }
}
