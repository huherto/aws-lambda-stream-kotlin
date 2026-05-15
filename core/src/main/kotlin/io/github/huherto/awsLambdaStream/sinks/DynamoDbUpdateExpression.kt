package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue

/**
 * Represents the components required to perform a DynamoDB update operation.
 *
 * This structure is intended to be passed to DynamoDB update APIs, where:
 * - [expressionAttributeNames] contains aliases for attribute names.
 * - [expressionAttributeValues] contains values referenced by the update expression.
 * - [updateExpression] contains the DynamoDB update expression string.
 * - [returnValues] controls which values DynamoDB returns after the update.
 *
 * By default, [returnValues] is set to `"ALL_NEW"`, meaning DynamoDB returns all
 * attributes of the item as they appear after the update.
 */
data class DynamoDbUpdateExpression(
    val expressionAttributeNames: Map<String, String>,
    val expressionAttributeValues: Map<String, AttributeValue>,
    val updateExpression: String,
    val returnValues: String = "ALL_NEW",
)

/**
 * Describes a single DynamoDB update operation for an attribute.
 *
 * DynamoDB supports multiple update actions:
 * - [DbSet] assigns or replaces an attribute value.
 * - [DbRemove] removes an attribute from the item.
 * - [DbAdd] adds a numeric value to an existing number or adds elements to a set.
 * - [DbDelete] removes elements from a set.
 */
sealed interface DynamoDbUpdateValue {
    /**
     * Sets an attribute to the provided [value].
     */
    data class DbSet(val value: AttributeValue) : DynamoDbUpdateValue

    /**
     * Removes an attribute from the item.
     */
    data object DbRemove : DynamoDbUpdateValue

    /**
     * Adds the provided [value] to an existing number or set attribute.
     */
    data class DbAdd(val value: AttributeValue) : DynamoDbUpdateValue

    /**
     * Deletes the provided [value] from an existing set attribute.
     */
    data class DbDelete(val value: AttributeValue) : DynamoDbUpdateValue
}

/**
 * Builds a DynamoDB update expression from a map of attribute names to update operations.
 *
 * Attribute names are automatically converted into expression attribute name placeholders
 * to avoid conflicts with DynamoDB reserved words or special characters.
 *
 * For example, an attribute named `"user-name"` is converted into a safe placeholder and
 * included in [DynamoDbUpdateExpression.expressionAttributeNames].
 *
 * @param item map of attribute names to their desired DynamoDB update operations.
 * @return a [DynamoDbUpdateExpression] containing the generated update expression,
 * attribute name placeholders, and attribute value placeholders.
 */
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
                is DynamoDbUpdateValue.DbSet -> {
                    expressionAttributeValues[valuePlaceholder] = updateValue.value
                    setClauses += "$namePlaceholder = $valuePlaceholder"
                }

                DynamoDbUpdateValue.DbRemove -> {
                    removeClauses += namePlaceholder
                }

                is DynamoDbUpdateValue.DbAdd -> {
                    expressionAttributeValues[valuePlaceholder] = updateValue.value
                    addClauses += "$namePlaceholder $valuePlaceholder"
                }

                is DynamoDbUpdateValue.DbDelete -> {
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

/**
 * Creates a DynamoDB-safe alias from an attribute name.
 *
 * Characters other than letters, digits, and underscores are replaced with a hexadecimal
 * escape sequence. This allows attribute names containing special characters to be used
 * safely in DynamoDB expressions.
 *
 * @param baseKey the original attribute name.
 * @return a safe alias that can be used as an expression attribute name or value suffix.
 */
private fun makeAliasForKey(baseKey: String): String =
    baseKey.replace(Regex("[^a-zA-Z0-9_]")) { match ->
        "_x${match.value.first().code.toString(16)}_"
    }

/**
 * Creates a DynamoDB condition expression that only allows an update when the existing
 * timestamp is missing or older than the incoming timestamp value.
 *
 * The returned map contains a `ConditionExpression` entry. The caller is expected to
 * provide matching expression attribute names and values for the generated placeholders.
 *
 * @param fieldName the timestamp attribute name to compare.
 * @return a map containing the DynamoDB condition expression.
 */
fun timestampCondition(fieldName: String = "timestamp"): Map<String, String> =
    mapOf(
        "ConditionExpression" to "attribute_not_exists(#$fieldName) OR #$fieldName < :$fieldName",
    )

/**
 * Creates a DynamoDB condition expression that only allows an update when the primary key
 * attribute does not already exist.
 *
 * This is useful for conditional writes where an item should only be created if it does
 * not already exist.
 *
 * @param fieldName the primary key attribute name.
 * @return a map containing the DynamoDB condition expression.
 */
fun pkCondition(fieldName: String = "pk"): Map<String, String> =
    mapOf(
        "ConditionExpression" to "attribute_not_exists($fieldName)",
    )
