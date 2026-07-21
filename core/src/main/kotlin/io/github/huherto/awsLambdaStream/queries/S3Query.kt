package io.github.huherto.awsLambdaStream.queries

import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.copyS3
import kotlinx.coroutines.flow.Flow

class S3Query(val s3Connector: S3Connector) {

    fun getObjectAsByteArray(fm: FaultManager, source: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        return fm.mapNotFaultyFrom(source) { uow ->
            val request = uow.s3.getRequest ?: return@mapNotFaultyFrom uow
            val response = s3Connector.getObjectAsByteArray(request, uow)

            uow.copyS3 {
                copy(getResponseBytes = response)
            }
        }
    }

    fun getObject(fm: FaultManager, source:  Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fm.mapNotFaultyFrom(source) { uow ->
            val request = uow.s3.getRequest ?: return@mapNotFaultyFrom uow
            val response = s3Connector.getObject(request, uow)

            uow.copyS3 {
                copy(getResponse = response)
            }
        }
    }

}
