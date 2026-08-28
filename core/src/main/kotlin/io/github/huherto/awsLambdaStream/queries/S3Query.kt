package io.github.huherto.awsLambdaStream.queries

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.extensions.copyS3
import io.github.huherto.awsLambdaStream.extensions.s3
import io.github.huherto.awsLambdaStream.faults.FaultManager
import kotlinx.coroutines.flow.Flow

class S3Query(val s3ConnectorOptions: S3Connector.Options) {

    fun getConnector() : S3Connector {
        return S3Connector(s3ConnectorOptions)
    }

    fun getObjectAsByteArray(fm: FaultManager, source: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        return fm.mapNotFaultyFrom(source) { uow ->
            val request = uow.s3.getRequest ?: return@mapNotFaultyFrom uow
            val response = getConnector().getObjectAsByteArray(request, uow)

            uow.copyS3 {
                copy(getResponseBytes = response)
            }
        }
    }

    fun getObject(fm: FaultManager, source:  Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fm.mapNotFaultyFrom(source) { uow ->
            val request = uow.s3.getRequest ?: return@mapNotFaultyFrom uow
            val response = getConnector().getObject(request, uow)

            uow.copyS3 {
                copy(getResponse = response)
            }
        }
    }

}
