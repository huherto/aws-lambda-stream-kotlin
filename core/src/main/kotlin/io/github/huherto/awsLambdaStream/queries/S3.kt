package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.copyS3
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

fun toGetObjectRequest(
    uow: UnitOfWork,
    bucketName: String,
    objectKey: String,
): UnitOfWork =
    uow.copyS3 {
        copy(
            getRequest = GetObjectRequest {
                bucket = bucketName
                key = objectKey
            }
        )
    }

fun toGetObjectRequest(
    bucketName: (UnitOfWork) -> String,
    objectKey: (UnitOfWork) -> String,
): (UnitOfWork) -> UnitOfWork = { uow ->
    uow.copyS3 {
        copy(
            getRequest = GetObjectRequest {
                bucket = bucketName(uow)
                key = objectKey(uow)
            }
        )
    }
}

fun toGetObjectRequest2(
    bucketName: (UnitOfWork) -> String,
    objectKey: (UnitOfWork) -> String,
): (UnitOfWork) -> UnitOfWork = { uow ->
    uow.copyS3 {
        copy(
            getRequest = GetObjectRequest {
                bucket = bucketName(uow)
                key = objectKey(uow)
            }
        )
    }
}

fun Flow<UnitOfWork>.getObjectFromS3(
    s3Connector: S3Connector,
): Flow<UnitOfWork> =
    map { uow ->
        val request = uow.s3.getRequest ?: return@map uow
        val response = s3Connector.getObject(request, uow)

        uow.copyS3 {
            copy(getResponse = response)
        }
    }

@OptIn(ExperimentalCoroutinesApi::class)
fun Flow<UnitOfWork>.getObjectFromS3AsStream(
    s3Connector: S3Connector,
    delimiter: String = "\n",
    splitFilter: suspend (String) -> Boolean = { true },
): Flow<UnitOfWork> =
    flatMapConcat { uow ->
        flow {
            val request = uow.s3.getRequest

            if (request == null) {
                emit(uow)
                return@flow
            }

            val text = s3Connector.getObjectAsText(request, uow)

            text
                .split(delimiter)
                .filter { splitFilter(it) }
                .forEach { getResponse ->
                    emit(
                        uow.copyS3 {
                            copy(getResponseText = getResponse)
                        }
                    )
                }
        }
    }

fun Flow<UnitOfWork>.getObjectFromS3AsByteArray(
    s3Connector: S3Connector,
): Flow<UnitOfWork> =
    map { uow ->
        val request = uow.s3.getRequest ?: return@map uow
        val response = s3Connector.getObjectAsByteArray(request, uow)

        uow.copyS3 {
            copy(getResponseBytes = response)
        }
    }

@OptIn(ExperimentalCoroutinesApi::class)
fun Flow<UnitOfWork>.splitS3Object(
    delimiter: String = "\n",
): Flow<UnitOfWork> =
    flatMapConcat { uow ->
        flow {
            val body = uow.s3.getResponseBytes

            if (body == null) {
                emit(uow)
                return@flow
            }

            body
                .decodeToString()
                .split(delimiter)
                .filter { it.isNotEmpty() }
                .forEach { line ->
                    emit(
                        uow.copyS3 {
                            copy(getResponseText = line)
                        }
                    )
                }
        }
    }

fun Flow<UnitOfWork>.listObjectsFromS3(
    s3Connector: S3Connector,
): Flow<UnitOfWork> =
    map { uow ->
        val request = uow.s3.listRequest ?: return@map uow
        val response = s3Connector.listObjects(request, uow)

        uow.copyS3 {
            copy(listResponse = response)
        }
    }

@OptIn(ExperimentalCoroutinesApi::class)
fun Flow<UnitOfWork>.pageObjectsFromS3(
    s3Connector: S3Connector,
    debug: (Any?) -> Unit = {},
): Flow<UnitOfWork> =
    flatMapConcat { uow ->
        flow {
            val baseRequest = uow.s3.listRequest

            if (baseRequest == null) {
                emit(uow)
                return@flow
            }

            var continuationToken = baseRequest.continuationToken

            do {
                val request = baseRequest.copy {
                    this.continuationToken = continuationToken
                }

                val response = s3Connector.listObjects(request, uow)

                debug(
                    mapOf(
                        "isTruncated" to response.isTruncated,
                        "nextContinuationToken" to response.nextContinuationToken,
                        "keyCount" to response.keyCount,
                        "name" to response.name,
                        "prefix" to response.prefix,
                    )
                )

                response.contents?.forEach { obj ->
                    emit(
                        uow.copyS3 {
                            copy(
                                listRequest = request,
                                listResponse = response,
                                listResponseObject = obj,
                            )
                        }
                    )
                }

                continuationToken =
                    if (response.isTruncated == true) {
                        response.nextContinuationToken
                    } else {
                        null
                    }
            } while (continuationToken != null)
        }
    }

fun Flow<UnitOfWork>.headS3Object(
    s3Connector: S3Connector,
): Flow<UnitOfWork> =
    map { uow ->
        val request = uow.s3.headRequest ?: return@map uow
        val response = s3Connector.headObject(request, uow)

        uow.copyS3 {
            copy(headResponse = response)
        }
    }

fun toListObjectsRequest(
    uow: UnitOfWork,
    bucketName: String,
    prefix: String? = null,
): UnitOfWork =
    uow.copyS3 {
        copy(
            listRequest = ListObjectsV2Request {
                bucket = bucketName
                this.prefix = prefix
            }
        )
    }