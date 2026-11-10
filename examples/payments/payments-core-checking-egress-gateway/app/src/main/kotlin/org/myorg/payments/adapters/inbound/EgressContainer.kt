package org.myorg.payments.adapters.inbound

import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilters
import org.myorg.payments.ActionPipeline
import org.myorg.payments.PaymentEvent
import org.myorg.payments.PaymentsEvent
import org.myorg.payments.PaymentsEventCodec
import org.myorg.payments.Simulation
import org.myorg.payments.SqsDelayedDelivery
import org.myorg.payments.adapters.outbound.DynamoDbBreakerStore
import org.myorg.payments.adapters.outbound.DynamoDbClaims
import org.myorg.payments.adapters.outbound.DynamoDbSimulatedOnce
import org.myorg.payments.adapters.outbound.DynamoDbSimulatedOutage
import org.myorg.payments.adapters.outbound.HttpCoreChecking
import org.myorg.payments.adapters.outbound.LedgerPostings
import org.myorg.payments.adapters.outbound.SimulatedCoreChecking
import org.myorg.payments.application.Breaker
import org.myorg.payments.application.CallNotSent
import org.myorg.payments.application.CalledOnce
import org.myorg.payments.application.ClosedHours
import org.myorg.payments.application.CoreCheckingUnavailable
import kotlin.time.Duration.Companion.seconds

/**
 * The egress side's driving adapter: kopipes pipelines that turn a request heard on the default channel into a call
 * of the called-once policy. `build()` is the composition root: it picks the real or the simulated core checking and
 * plugs the DynamoDB adapters into the ports.
 */
class EgressContainer(val calledOnce: CalledOnce) {
    companion object {
        fun build(): EgressContainer {
            installCoreCheckingFaultManager()
            val dynamoDb = AwsGlobalRegistry.dynamoDbClientFactory().getClient("calls")
            val callsTable = System.getenv("CALLS_TABLE_NAME") ?: error("CALLS_TABLE_NAME is not configured")
            val coreChecking =
                if (Simulation.enabled()) SimulatedCoreChecking(
                    SqsDelayedDelivery.fromEnvironment(),
                    Simulation.delaySeconds(2),
                    postings = LedgerPostings(dynamoDb, System.getenv("LEDGER_TABLE_NAME") ?: error("LEDGER_TABLE_NAME is not configured")),
                    lost = DynamoDbSimulatedOnce(dynamoDb, callsTable),
                    outage = DynamoDbSimulatedOutage(
                        dynamoDb, callsTable,
                        lasts = (System.getenv("SIMULATED_OUTAGE_SECONDS")?.toLongOrNull() ?: 60).seconds,
                    ),
                )
                else HttpCoreChecking(System.getenv("CORE_CHECKING_URL") ?: error("CORE_CHECKING_URL is not configured"))
            val claims = DynamoDbClaims(dynamoDb, callsTable)
            val grace = System.getenv("CALL_GRACE_SECONDS")?.toLongOrNull() ?: 300
            val breaker = Breaker(
                store = DynamoDbBreakerStore(dynamoDb, callsTable),
                threshold = System.getenv("BREAKER_THRESHOLD")?.toIntOrNull() ?: 3,
                openFor = (System.getenv("BREAKER_OPEN_SECONDS")?.toLongOrNull() ?: 30).seconds,
                closedHours = ClosedHours.parse(System.getenv("CORE_CHECKING_CLOSED_HOURS")),
            )
            return EgressContainer(CalledOnce(coreChecking, claims, grace = grace.seconds, breaker = breaker))
        }
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(
            ActionPipeline.builder()
                .id("withdraw")
                .eventFilter(EventFilters.name(PaymentsEvent.WITHDRAWAL_REQUESTED))
                .action { uow -> calledOnce.debit((uow.event as PaymentEvent).payment, request = uow.event.toString()) }
                .build()
        )
        .addPipeline(
            ActionPipeline.builder()
                .id("refund")
                .eventFilter(EventFilters.name(PaymentsEvent.REFUND_REQUESTED))
                .action { uow -> calledOnce.credit((uow.event as PaymentEvent).payment, request = uow.event.toString()) }
                .build()
        )
        .build()

    val kinesisAdapter = KinesisAdapter(eventCodec = PaymentsEventCodec)
}

/**
 * While core checking is down or refuses the connection, requests are retried by the stream
 * (with `STREAM_RETRY_ENABLED=true`) instead of becoming faults. A call unresolved past the grace period does become
 * a fault, for an operator.
 */
fun installCoreCheckingFaultManager() {
    AwsGlobalRegistry.setFaultManagerFactory {
        FaultManager(
            eventPublisher = AwsGlobalRegistry.eventPublisher(),
            isRetryable = {
                it is CallNotSent || it is CoreCheckingUnavailable || AwsGlobalRegistry.isAwsRetryable(it)
            },
        )
    }
}
