package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.extensions.copyS3
import io.github.huherto.awsLambdaStream.extensions.s3
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.Serializable

// claim-check pattern support
// https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html

@Serializable
data class ClaimCheck(
    val bucket: String,
    val key: String,
)

class ClaimCheckRedeemer(
    s3Connector: S3Connector,
    private val faultManager: FaultManager,
    private val eventCodec: EventCodec,
    private val claimCheck: (UnitOfWork) -> ClaimCheck? = { uow ->
        uow.event?.raw as? ClaimCheck
    }) {

    private val s3Query = S3Query(s3Connector)

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

