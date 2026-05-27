package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.copyS3
import kotlinx.coroutines.flow.Flow

// claim-check pattern support
// https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html

data class ClaimCheck(
    val bucket: String,
    val key: String,
)

class ClaimCheckRedeemer(
    private val s3Connector: S3Connector,
    private val faultManager: FaultManager,
    private val eventCodec: EventCodec,
    private val claimCheck: (UnitOfWork) -> ClaimCheck? = { uow ->
        uow.event?.raw as? ClaimCheck
    }) {

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
                .getObjectFromS3AsByteArray(s3Connector)
                .mapNotFaulty { uow ->
                    val body = uow.s3.getResponseBytes

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
        if (uow.s3.getRequest == null) {
            // Backwards compatibility with the TypeScript behavior:
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

