package org.myorg.payments

import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import mu.KotlinLogging

/** The bank network, a batch system: payments go out in tonight's transfer file. */
fun interface BankNetwork {
    suspend fun transfer(payment: Payment)
}

/**
 * The real network: adds the payment's line to tonight's transfer file, one object per payment keyed by its id,
 * so a payment appears once even if DEPOSIT_REQUESTED arrives twice. Not deployed in this example; still to build:
 * the nightly job that joins tonight's lines into `PAY_<date>.csv` and sends it at the network's cut-off.
 */
class S3TransferFile(private val bucket: String, private val clock: () -> Long = System::currentTimeMillis) : BankNetwork {
    override suspend fun transfer(payment: Payment) {
        val client = AwsGlobalRegistry.s3ClientFactory().getClient("transfers")
        client.putObject(PutObjectRequest {
            this.bucket = this@S3TransferFile.bucket
            key = "outbound/PAY_${fileDate(clock())}/${payment.id}.csv"
            body = ByteStream.fromString(transferLine(payment) + "\n")
        })
    }
}

/**
 * Simulation mode: the transfer file isn't sent. After the delay the settlement the network would have returned
 * is dropped where the ingress gateway picks up files. B-7 settles after the control service's deadline.
 */
class SimulatedBankNetwork(
    private val delivery: DelayedDelivery,
    private val delaySeconds: Int,
    private val lateDelaySeconds: Int,
) : BankNetwork {
    override suspend fun transfer(payment: Payment) {
        val delay = if (payment.to.account == Simulation.LATE_BANK_ACCOUNT) lateDelaySeconds else delaySeconds
        val line = Json.encodeToString(SettlementLine.serializer(), simulatedSettlement(payment))
        // B-6: the network drops the same settlement file (same name, same content) more than once.
        repeat(Simulation.deliveries(payment.to.account)) { n ->
            delivery.deliverLater(line, delay + n * Simulation.REDELIVERY_SECONDS)
        }
    }
}

class EgressContainer(val bankNetwork: BankNetwork) {
    companion object {
        fun build(): EgressContainer = EgressContainer(
            if (Simulation.enabled()) SimulatedBankNetwork(
                SqsDelayedDelivery.fromEnvironment(),
                delaySeconds = Simulation.delaySeconds(20),
                lateDelaySeconds = System.getenv("SIMULATED_LATE_DELAY_SECONDS")?.toIntOrNull() ?: 180,
            )
            else S3TransferFile(System.getenv("OUTBOUND_BUCKET_NAME") ?: error("OUTBOUND_BUCKET_NAME is not configured")),
        )
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(
            ActionPipeline.builder()
                .id("transfer")
                .eventFilter(depositsTo(Party.BANK))
                .action { uow -> bankNetwork.transfer((uow.event as PaymentEvent).payment) }
                .build()
        )
        .build()

    val kinesisAdapter = KinesisAdapter(eventCodec = PaymentsEventCodec)
}

class Listener(
    containerFactory: () -> EgressContainer = { EgressContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {
    private val container by loggedLazy("EgressContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        runKinesis(container.assembler, container.kinesisAdapter, event)
        return null
    }
}

/** Simulation only: drops `SETTLE_<date>_<ref>.csv` into the bucket the ingress gateway watches. */
class Simulator : RequestHandler<SQSEvent, Void?> {
    private val logger = KotlinLogging.logger { }
    private val inbound = System.getenv("INBOUND_BUCKET_NAME") ?: "undefined"

    override fun handleRequest(event: SQSEvent, context: Context): Void? = runBlocking {
        val client = AwsGlobalRegistry.s3ClientFactory().getClient("simulator")
        for (message in event.records) {
            val line = Json.decodeFromString(SettlementLine.serializer(), message.body)
            val key = "inbound/SETTLE_${fileDate(System.currentTimeMillis())}_${line.reference}.csv"
            client.putObject(PutObjectRequest {
                bucket = inbound
                this.key = key
                body = ByteStream.fromString(settlementFile(listOf(line)))
            })
            logger.info { "simulated settlement file $key: ${line.toCsv()}" }
        }
        null
    }
}
