package io.github.huherto.awsLambdaStream.queries

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.copyS3
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

class S3Query(val s3Connector: S3Connector) {

    fun getObjectAsByteArray(fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        return fromFlow.map { uow ->
            val request = uow.s3.getRequest ?: return@map uow
            val response = s3Connector.getObjectAsByteArray(request, uow)

            uow.copyS3 {
                copy(getResponseBytes = response)
            }
        }
    }

    fun getObject(fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow.map { uow ->
            val request = uow.s3.getRequest ?: return@map uow
            val response = s3Connector.getObject(request, uow)

            uow.copyS3 {
                copy(getResponse = response)
            }
        }
    }

}
