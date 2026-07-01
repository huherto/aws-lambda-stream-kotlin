package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import kotlinx.coroutines.runBlocking

class CheckHealthApi(
    private val container: CheckHealthApiContainer = CheckHealthApiContainer.build(),
) : RequestHandler<Map<String, Any?>, Any?> {

    override fun handleRequest(
        event: Map<String, Any?>,
        context: Context,
    ): Any? = runBlocking {
        val debug : (String) -> Unit = debug("handler")

        debug("event: $event")

        when (event.routeKey()) {
            "GET /check", "/check", "GET:/check" -> {
                val checkResponse = container.tracerDao.check(container.unhealthyFlag)

                mapOf(
                    "statusCode" to checkResponse.statusCode,
                    "headers" to mapOf(
                        "Cache-Control" to "no-cache, no-store, must-revalidate",
                    ),
                    "body" to checkResponse,
                )
            }
            else -> mapOf(
                "statusCode" to 404,
                "body" to "Not Found",
            )
        }
    }
}

private fun debug(namespace: String): (String) -> Unit =
    { message ->
        println("[$namespace] $message")
    }

private fun Map<String, Any?>.routeKey(): String? {
    val routeKey = this["routeKey"] as? String
    if (routeKey != null) return routeKey

    val httpMethod = this["httpMethod"] as? String
    val path = this["path"] as? String

    if (httpMethod != null && path != null) {
        return "$httpMethod $path"
    }

    val requestContext = this["requestContext"] as? Map<*, *>
    val http = requestContext?.get("http") as? Map<*, *>
    val method = http?.get("method") as? String
    val rawPath = this["rawPath"] as? String

    return if (method != null && rawPath != null) {
        "$method $rawPath"
    } else {
        path ?: rawPath
    }
}