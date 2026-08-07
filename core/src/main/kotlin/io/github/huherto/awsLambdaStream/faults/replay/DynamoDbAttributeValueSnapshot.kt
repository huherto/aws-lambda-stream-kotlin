package io.github.huherto.awsLambdaStream.faults.replay

import kotlinx.serialization.Serializable

@Serializable
data class DynamoDbAttributeValueSnapshot(
    val S: String? = null,
    val N: String? = null,
    val B: String? = null,
    val BOOL: Boolean? = null,
    val NULL: Boolean? = null,
    val M: Map<String, DynamoDbAttributeValueSnapshot>? = null,
    val L: List<DynamoDbAttributeValueSnapshot>? = null,
    val SS: List<String>? = null,
    val NS: List<String>? = null,
    val BS: List<String>? = null,
)
