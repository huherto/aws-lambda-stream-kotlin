package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.extensions.copyS3
import io.github.huherto.awsLambdaStream.extensions.s3
import io.github.huherto.awsLambdaStream.faults.FaultManager
import kotlinx.coroutines.flow.Flow

// claim-check pattern support
// https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html

typealias ClaimCheck = io.github.huherto.awsLambdaStream.ClaimCheck

/** Implements the Claim-Check pattern by fetching full event payloads from S3. */
class ClaimCheckRedeemer(
    s3ConnectorOptions: S3Connector.Options = S3Connector.Options(),
    private val faultManager: FaultManager = GlobalRegistry.faultManager(),
    private val eventCodec: EventCodec,
    private val claimCheck: (UnitOfWork) -> ClaimCheck? = { uow ->
        uow.event?.raw as? ClaimCheck
    }) {

    private val s3Query = S3Query(s3ConnectorOptions)

    fun Flow<UnitOfWork>.redeemClaimCheck(): Flow<UnitOfWork> {
        return with(faultManager) {
            mapNotFaulty { uow ->
                val request = claimCheck(uow)?.let { claimCheck ->
                    GetObjectRequest {
                        bucket = claimCheck.bucket
                        key = claimCheck.key
                    }
                }

                uow.copyS3 {
                    copy(getRequest = request)
                }
            }
                .let { s3Query.getObjectAsByteArray(faultManager, it) }
                .mapNotFaulty { uow ->
                    val body = uow.s3?.getResponseBytes

                    if (body == null) {
                        clearClaimCheck(uow)
                    } else {
                        val event = eventCodec.decode(body.decodeToString())
                        clearClaimCheck(uow.copy(event = event))
                    }
                }
        }
    }

    private fun clearClaimCheck(uow: UnitOfWork): UnitOfWork {
        if (uow.s3?.getRequest == null) {
            // when no claim-check request was created, remove transient S3 response state too.
            return uow.copyS3 {
                copy(
                    getRequest = null,
                    getResponse = null,
                    getResponseText = null,
                    getResponseBytes = null,
                )
            }
        }

        return uow
    }
}

