package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.extensions.copyS3
import io.github.huherto.awsLambdaStream.extensions.s3
import io.github.huherto.awsLambdaStream.metrics.withStepMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

class S3Sink(
    private val envConfig: EnvironmentConfig,
    private val s3Connector: S3Connector,
    private val bucketName: String = envConfig.bucketName() ?: error("bucketName is not set"),
) {

    fun Flow<UnitOfWork>.rateLimit(): Flow<UnitOfWork> = this

    fun ensurePutRequestBucket(uow: UnitOfWork): UnitOfWork  {
        val s3 = uow.s3
        val putRequest = s3.putRequest
        if (putRequest != null) {
            if (putRequest.bucket == null) {
                return uow.copyS3 { copy(putRequest = putRequest.copy { bucket = bucketName }) }
            }
        }
        return uow
    }

    fun ensureDeleteRequestBucket(uow: UnitOfWork): UnitOfWork  {
        val s3 = uow.s3
        val deleteRequest = s3.deleteRequest
        if (deleteRequest != null) {
            if (deleteRequest.bucket == null) {
                return uow.copyS3 { copy(deleteRequest = deleteRequest.copy { bucket = bucketName }) }
            }
        }
        return uow
    }

    fun ensureCopyRequestBucket(uow: UnitOfWork): UnitOfWork  {
        val s3 = uow.s3
        val copyRequest = s3.copyRequest
        if (copyRequest != null) {
            if (copyRequest.bucket == null) {
                return uow.copyS3 { copy(copyRequest = copyRequest.copy { bucket = bucketName }) }
            }
        }
        return uow
    }

    fun putObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.rateLimit()
            .map { uow -> ensurePutRequestBucket(uow) }
            .map { uow ->
                val request = uow.s3?.putRequest ?: return@map uow
                uow.withStepMetrics("save", envConfig) { uowWithMetrics ->
                    val response = s3Connector.putObject(uowWithMetrics.s3.putRequest!!, uowWithMetrics)
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
                val request = uow.s3?.deleteRequest ?: return@map uow
                uow.withStepMetrics("delete", envConfig) { uowWithMetrics ->
                    val response = s3Connector.deleteObject(uowWithMetrics.s3.deleteRequest!!, uowWithMetrics)
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
                val request = uow.s3?.copyRequest ?: return@map uow
                uow.withStepMetrics("copy", envConfig) { uowWithMetrics ->
                    val response = s3Connector.copyObject(uowWithMetrics.s3.copyRequest!!, uowWithMetrics)
                    uowWithMetrics.copyS3 {
                        copy(copyResponse = response)
                    }
                }
            }
    }
}