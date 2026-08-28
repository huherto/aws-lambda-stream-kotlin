package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.RawRecord
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.extensions.copyS3
import io.github.huherto.awsLambdaStream.extensions.s3
import io.github.huherto.awsLambdaStream.faults.FaultManager
import kotlinx.coroutines.flow.Flow

// claim-check pattern support
// https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html

/**
 * Kept as an alias so existing `io.github.huherto.awsLambdaStream.queries.ClaimCheck` imports
 * keep working. The declaration moved to the root package because Kotlin requires the members
 * of the sealed [RawRecord] hierarchy to share its package.
 */
typealias ClaimCheck = io.github.huherto.awsLambdaStream.ClaimCheck

/**
 * `ClaimCheckRedeemer` implements the Claim-Check pattern by fetching the full event
 * payload from S3 when a [ClaimCheck] is present in the record.
 *
 * @param s3ConnectorOptions Options for the [S3Connector] used to fetch the payload.
 * @param faultManager The [io.github.huherto.awsLambdaStream.faults.FaultManager] used to handle errors during redemption.
 * @param eventCodec The [EventCodec] used to decode the fetched payload.
 * @param claimCheck A function that extracts the [ClaimCheck] from a [UnitOfWork].
 */
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

