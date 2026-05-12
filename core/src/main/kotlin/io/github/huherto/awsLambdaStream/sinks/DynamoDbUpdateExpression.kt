package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue

data class DynamoDbUpdateExpression(
    val expressionAttributeNames: Map<String, String>,
    val expressionAttributeValues: Map<String, AttributeValue>,
    val updateExpression: String,
    val returnValues: String = "ALL_NEW",
)

sealed interface DynamoDbUpdateValue {
    data class Set(val value: AttributeValue) : DynamoDbUpdateValue
    data object Remove : DynamoDbUpdateValue
    data class Add(val value: AttributeValue) : DynamoDbUpdateValue
    data class Delete(val value: AttributeValue) : DynamoDbUpdateValue
}

fun updateExpression(item: Map<String, DynamoDbUpdateValue>): DynamoDbUpdateExpression {
    val expressionAttributeNames = mutableMapOf<String, String>()
    val expressionAttributeValues = mutableMapOf<String, AttributeValue>()

    val setClauses = mutableListOf<String>()
    val addClauses = mutableListOf<String>()
    val deleteClauses = mutableListOf<String>()
    val removeClauses = mutableListOf<String>()

    item
        .forEach { (attributeName, updateValue) ->
            val alias = makeAliasForKey(attributeName)
            val namePlaceholder = "#$alias"
            val valuePlaceholder = ":$alias"

            expressionAttributeNames[namePlaceholder] = attributeName

            when (updateValue) {
                is DynamoDbUpdateValue.Set -> {
                    expressionAttributeValues[valuePlaceholder] = updateValue.value
                    setClauses += "$namePlaceholder = $valuePlaceholder"
                }

                DynamoDbUpdateValue.Remove -> {
                    removeClauses += namePlaceholder
                }

                is DynamoDbUpdateValue.Add -> {
                    expressionAttributeValues[valuePlaceholder] = updateValue.value
                    addClauses += "$namePlaceholder $valuePlaceholder"
                }

                is DynamoDbUpdateValue.Delete -> {
                    expressionAttributeValues[valuePlaceholder] = updateValue.value
                    deleteClauses += "$namePlaceholder $valuePlaceholder"
                }
            }
        }

    val updateExpressionParts = buildList {
        if (setClauses.isNotEmpty()) add("SET ${setClauses.joinToString(", ")}")
        if (removeClauses.isNotEmpty()) add("REMOVE ${removeClauses.joinToString(", ")}")
        if (addClauses.isNotEmpty()) add("ADD ${addClauses.joinToString(", ")}")
        if (deleteClauses.isNotEmpty()) add("DELETE ${deleteClauses.joinToString(", ")}")
    }

    return DynamoDbUpdateExpression(
        expressionAttributeNames = expressionAttributeNames,
        expressionAttributeValues = expressionAttributeValues,
        updateExpression = updateExpressionParts.joinToString(" "),
    )
}

private fun makeAliasForKey(baseKey: String): String =
    baseKey.replace(Regex("[^a-zA-Z0-9_]")) { match ->
        "_x${match.value.first().code.toString(16)}_"
    }

fun timestampCondition(fieldName: String = "timestamp"): Map<String, String> =
    mapOf(
        "ConditionExpression" to "attribute_not_exists(#$fieldName) OR #$fieldName < :$fieldName",
    )

fun pkCondition(fieldName: String = "pk"): Map<String, String> =
    mapOf(
        "ConditionExpression" to "attribute_not_exists($fieldName)",
    )
