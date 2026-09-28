package io.kopipes.aws.queries

import io.kopipes.aws.UnitOfWork
import io.kopipes.aws.connectors.S3Connector
import io.kopipes.aws.extensions.copyS3
import io.kopipes.aws.extensions.s3
import io.kopipes.aws.faults.FaultManager
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
