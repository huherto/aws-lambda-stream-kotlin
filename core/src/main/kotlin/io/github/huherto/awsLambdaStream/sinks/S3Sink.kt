package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.extensions.copyS3
import io.github.huherto.awsLambdaStream.extensions.s3
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
            val response = s3Connector.putObject(request, uow)

            uow.copyS3 {
                copy(putResponse = response)
            }
        }
    }

    fun deleteObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.rateLimit()
            .map { uow -> ensureDeleteRequestBucket(uow) }
            .map { uow ->
                val request = uow.s3?.deleteRequest ?: return@map uow
                val response = s3Connector.deleteObject(request, uow)

                uow.copyS3 {
                    copy(deleteResponse = response)
                }
            }
    }

    fun copyObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.rateLimit()
            .map { uow -> ensureCopyRequestBucket(uow) }
            .map { uow ->
            val request = uow.s3?.copyRequest ?: return@map uow
            val response = s3Connector.copyObject(request, uow)

            uow.copyS3 {
                copy(copyResponse = response)
            }
        }
    }
}