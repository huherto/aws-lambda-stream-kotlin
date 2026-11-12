package org.myorg.payments.adapters.inbound

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.myorg.payments.application.PaymentKey
import org.myorg.payments.application.Submission
import org.myorg.payments.domain.PaymentView
import org.myorg.payments.domain.Rejection
import org.myorg.payments.domain.Status
import java.util.Base64

/** The one error shape of the API. */
@Serializable
data class ApiError(val error: String, val message: String, val field: String? = null) {
    constructor(rejection: Rejection) : this(rejection.error, rejection.message, rejection.field)
}

/** A page of a customer's payments, newest first. `next` is absent on the last page. */
@Serializable
data class PaymentsPageView(val items: List<PaymentView>, val next: String? = null)

/**
 * The customer's only entry point: a driving adapter that turns HTTP into calls on the `Payments` port and its
 * answers back into HTTP. Routing, headers, query parameters, cursors and status codes live here; the use cases don't.
 *
 * | Method and path                              | Response                                   |
 * |----------------------------------------------|--------------------------------------------|
 * | `PUT /payments/{id}`                         | `202` + the view (`status: SUBMITTED`)     |
 * | `GET /payments/{id}`                         | `200` + the view; `404` if unknown         |
 * | `GET /payments?status=…&limit=…&cursor=…`    | `200` + a page of views                    |
 *
 * The customer is the `X-Customer-Id` header: a stand-in for an access token, **not authentication**.
 */
class RestApi(
    containerFactory: () -> RestApiContainer = { RestApiContainer.build() },
) : RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private val logger = KotlinLogging.logger { }

    private val container: RestApiContainer by loggedLazy(
        name = "RestApiContainer",
        logger = logger,
        initializer = containerFactory,
    )

    override fun handleRequest(input: APIGatewayProxyRequestEvent, context: Context): APIGatewayProxyResponseEvent =
        runBlocking {
            logger.info { "${input.httpMethod} ${input.path}" }
            try {
                route(input)
            } catch (e: Exception) {
                logger.error(e) { "Unexpected error" }
                respond(500, ApiError("internal-error", "something went wrong"))
            }
        }

    private suspend fun route(input: APIGatewayProxyRequestEvent): APIGatewayProxyResponseEvent {
        val customerId = header(input, "X-Customer-Id")?.takeIf { it.isNotBlank() }
            ?: return respond(401, ApiError("unauthenticated", "X-Customer-Id is required"))

        return when (input.httpMethod to input.resource) {
            "PUT" to "/payments/{id}" -> put(customerId, input.pathParameters?.get("id"), input.body)
            "GET" to "/payments/{id}" -> get(customerId, input.pathParameters?.get("id"))
            "GET" to "/payments" -> list(customerId, input)
            else -> respond(404, ApiError("not-found", "no such resource"))
        }
    }

    private suspend fun put(customerId: String, id: String?, body: String?): APIGatewayProxyResponseEvent =
        when (val submission = container.payments.submit(customerId, id, body)) {
            is Submission.Accepted -> respond(202, submission.view)
            is Submission.Invalid -> respond(400, ApiError(submission.rejection))
            is Submission.Conflict ->
                respond(409, ApiError("conflict", "payment ${submission.id} was already submitted with a different body"))
        }

    private suspend fun get(customerId: String, id: String?): APIGatewayProxyResponseEvent {
        val view = id?.let { container.payments.find(customerId, it) }
            ?: return respond(404, ApiError("not-found", "no such payment"))
        return respond(200, view)
    }

    private suspend fun list(customerId: String, input: APIGatewayProxyRequestEvent): APIGatewayProxyResponseEvent {
        val params = input.multiValueQueryStringParameters.orEmpty()
        val single = input.queryStringParameters.orEmpty()

        val limitText = single["limit"] ?: params["limit"]?.firstOrNull()
        val limit = if (limitText == null) DEFAULT_LIMIT else limitText.toIntOrNull()?.takeIf { it in 1..MAX_LIMIT }
            ?: return respond(400, ApiError("invalid-request", "limit must be between 1 and $MAX_LIMIT", "limit"))

        val statusTexts = params["status"] ?: listOfNotNull(single["status"])
        val statuses = statusTexts.map { text ->
            Status.entries.firstOrNull { it.name == text }
                ?: return respond(400, ApiError("invalid-request", "unknown status $text", "status"))
        }.toSet()

        val cursorText = single["cursor"] ?: params["cursor"]?.firstOrNull()
        val cursor = cursorText?.let {
            decodeCursor(it) ?: return respond(400, ApiError("invalid-request", "cursor is not valid", "cursor"))
        }

        val page = container.payments.list(customerId, statuses, limit, cursor)
        return respond(200, PaymentsPageView(page.items, next = page.next?.let(::encodeCursor)))
    }

    private fun header(input: APIGatewayProxyRequestEvent, name: String): String? =
        input.headers?.entries?.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value
            ?: input.multiValueHeaders?.entries?.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value?.firstOrNull()

    companion object {
        const val DEFAULT_LIMIT = 20
        const val MAX_LIMIT = 100

        private val responseJson = Json { explicitNulls = false; encodeDefaults = true }

        internal inline fun <reified T> respond(status: Int, body: T): APIGatewayProxyResponseEvent =
            APIGatewayProxyResponseEvent()
                .withStatusCode(status)
                .withHeaders(mapOf("Content-Type" to "application/json"))
                .withBody(responseJson.encodeToString(body))
    }
}

/** Opaque to the client. */
fun encodeCursor(key: PaymentKey): String =
    Base64.getUrlEncoder().withoutPadding().encodeToString("${key.submittedAt}:${key.id}".toByteArray())

fun decodeCursor(cursor: String): PaymentKey? = runCatching {
    val (submittedAt, id) = String(Base64.getUrlDecoder().decode(cursor)).split(":", limit = 2)
    PaymentKey(id = id, submittedAt = submittedAt.toLong())
}.getOrNull()
