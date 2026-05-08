package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

data class DynamoDbUpdateExpression(
    val expressionAttributeNames: Map<String, String>,
    val expressionAttributeValues: Map<String, Any?>,
    val updateExpression: String,
    val returnValues: String = "ALL_NEW",
)

fun updateExpression(item: Map<String, Any?>): DynamoDbUpdateExpression {
    val expressionAttributeNames = mutableMapOf<String, String>()
    val expressionAttributeValues = mutableMapOf<String, Any?>()

    val setClauses = mutableListOf<String>()
    val addClauses = mutableListOf<String>()
    val deleteClauses = mutableListOf<String>()
    val removeClauses = mutableListOf<String>()

    item
        .filterValues { value -> value != null }
        .forEach { (key, value) ->
            val isDeleteSet = key.endsWith("_delete")
            val baseKey = if (isDeleteSet) key.removeSuffix("_delete") else key

            val alias = makeAliasForKey(baseKey)

            expressionAttributeNames["#$alias"] = baseKey

            when {
                value == null -> {
                    removeClauses += "#$alias"
                }

                isDeleteSet -> {
                    val setValue = if (value is Set<*>) value else setOf(value)
                    expressionAttributeValues[":${alias}_delete"] = setValue
                    deleteClauses += "#$alias :${alias}_delete"
                }

                value is Set<*> -> {
                    expressionAttributeValues[":$alias"] = value
                    addClauses += "#$alias :$alias"
                }

                else -> {
                    expressionAttributeValues[":$alias"] = value
                    setClauses += "#$alias = :$alias"
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

private fun makeAliasForKey(baseKey: String): String = baseKey.replace(Regex("[^a-zA-Z0-9_]")) { match ->
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

data class DynamoDbSinkOptions(
    val pipelineId: String? = null,
    val tableName: String? = System.getenv("ENTITY_TABLE_NAME") ?: System.getenv("EVENT_TABLE_NAME"),
    val updateRequestField: String = "updateRequest",
    val updateResponseField: String = "updateResponse",
    val putRequestField: String = "putRequest",
    val parallel: Int =
        System.getenv("UPDATE_PARALLEL")?.toIntOrNull()
            ?: System.getenv("PARALLEL")?.toIntOrNull()
            ?: 4,
    val timeout: Long =
        System.getenv("DYNAMODB_TIMEOUT")?.toLongOrNull()
            ?: System.getenv("TIMEOUT")?.toLongOrNull()
            ?: 1_000L,
    val removeUndefinedValues: Boolean = true,
    val throwConditionFailure: Boolean = false,
    val step: String = "save",
)

class DynamoDbSink(
    private val options: DynamoDbSinkOptions = DynamoDbSinkOptions(),
    private val connector: DynamoDbConnector,
) {
    fun update(source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source
            .rateLimit(options)
            .mapParallel(options.parallel) { uow ->
                val request = uow.updateRequest ?: return@mapParallel uow

                try {
                    val updateResponse =  connector.update(request, uow)

                    uow.copy(updateResponse = updateResponse)
                } catch (error: Throwable) {
                    rejectWithFault(uow, error)
                }
            }

    fun put(source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source
            .mapParallel(options.parallel) { uow ->
                val request = uow.putRequest ?: return@mapParallel uow

                try {
                    val putResponse = connector.put(request, uow)

                    uow.copy(putResponse = putResponse)
                } catch (error: Throwable) {
                    rejectWithFault(uow, error)
                }
            }
}

private fun Flow<UnitOfWork>.rateLimit(
    options: DynamoDbSinkOptions,
): Flow<UnitOfWork> {
    // Equivalent placeholder for the TypeScript `.through(ratelimit(opt))`.
    // Implement throttling here if your Kotlin pipeline has rate-limit options.
    return this
}

private fun <T, R> Flow<T>.mapParallel(
    parallelism: Int,
    transform: suspend (T) -> R,
): Flow<R> = channelFlow {
    val semaphore = Semaphore(parallelism)

    collect { value ->
        semaphore.withPermit {
            send(transform(value))
        }
    }
}.buffer(parallelism)

private suspend fun rejectWithFault(
    uow: UnitOfWork,
    error: Throwable,
): UnitOfWork {
    // Equivalent placeholder for `rejectWithFault(uow)` from TypeScript.
    // Replace with your project’s FaultManager/fault representation.
    throw error
}

private object Undefined