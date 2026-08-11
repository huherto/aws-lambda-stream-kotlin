package io.github.huherto.awsLambdaStream.serialization.snapshots

import kotlinx.serialization.Serializable

@Serializable
data class S3Snapshot(
    val bucket: String? = null,
    val key: String? = null,
    val getRequest: String? = null,
    val getResponse: String? = null,
    val deleteRequest: String? = null,
    val deleteResponse: String? = null,
    val copyRequest: String? = null,
    val copyResponse: String? = null,
    val getResponseText: String? = null,
    val getResponseBytes: ByteArray? = null,
    val putRequest: String? = null,
    val putResponse: String? = null,
    val listRequest: String? = null,
    val listResponse: String? = null,
    val listResponseObject: String? = null,
    val headRequest: String? = null,
    val headResponse: String? = null,
)
