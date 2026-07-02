package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.copyS3
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

class S3Sink(
    private val s3Connector: S3Connector,
) {

    fun putObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.map { uow ->
            val request = uow.s3.putRequest ?: return@map uow
            val response = s3Connector.putObject(request, uow)

            uow.copyS3 {
                copy(putResponse = response)
            }
        }
    }

    fun deleteObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.map { uow ->
            val request = uow.s3.deleteRequest ?: return@map uow
            val response = s3Connector.deleteObject(request, uow)

            uow.copyS3 {
                copy(deleteResponse = response)
            }
        }
    }

    fun copyObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.map { uow ->
            val request = uow.s3.copyRequest ?: return@map uow
            val response = s3Connector.copyObject(request, uow)

            uow.copyS3 {
                copy(copyResponse = response)
            }
        }
    }
}