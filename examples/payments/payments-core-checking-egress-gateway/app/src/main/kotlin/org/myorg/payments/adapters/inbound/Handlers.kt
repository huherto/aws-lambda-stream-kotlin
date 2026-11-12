package org.myorg.payments.adapters.inbound

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.myorg.payments.adapters.outbound.DynamoDbClaims
import org.myorg.payments.adapters.outbound.ListenerRedelivery
import org.myorg.payments.application.Reconciler
import org.myorg.payments.runKinesis

/** Default channel → core checking. */
class Listener(
    containerFactory: () -> EgressContainer = { EgressContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {
    private val container by loggedLazy("EgressContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        runKinesis(container.assembler, container.kinesisAdapter, event)
        return null
    }
}

/** Scheduled every minute: redelivers the requests in doubt to the listener. */
class ReconcilerHandler(
    factory: () -> Reconciler = ::reconciler,
) : RequestHandler<ScheduledEvent, Int> {
    private val reconciler by loggedLazy("Reconciler", KotlinLogging.logger { }, factory)

    override fun handleRequest(event: ScheduledEvent, context: Context): Int = runBlocking { reconciler.reconcile() }
}

private fun reconciler(): Reconciler {
    val dynamoDb = AwsKopipesContext().dynamoDbClientFactory.getClient("reconciler")
    val claims = DynamoDbClaims(dynamoDb, System.getenv("CALLS_TABLE_NAME") ?: error("CALLS_TABLE_NAME is not configured"))
    val listener = System.getenv("LISTENER_FUNCTION_NAME") ?: error("LISTENER_FUNCTION_NAME is not configured")
    return Reconciler(claims, ListenerRedelivery.lambda(listener))
}
