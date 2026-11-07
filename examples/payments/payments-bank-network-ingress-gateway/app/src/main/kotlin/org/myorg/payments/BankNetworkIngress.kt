package org.myorg.payments

import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.smithy.kotlin.runtime.content.decodeToString
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.JsonRaw
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.flow.asFlow
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import mu.KotlinLogging
import java.time.Instant

/** A settlement file that arrived where the gateway picks up files. */
data class ArrivedFile(val bucket: String, val key: String, val arrivedAt: Long)

@Serializable
private data class S3Notification(val Records: List<S3Record> = emptyList())

@Serializable
private data class S3Record(val eventTime: String? = null, val s3: S3Entity)

@Serializable
private data class S3Entity(val bucket: S3Bucket, val `object`: S3Object)

@Serializable
private data class S3Bucket(val name: String)

@Serializable
private data class S3Object(val key: String)

private val notificationJson = Json { ignoreUnknownKeys = true }

/** The files an S3 notification (delivered through SQS) announces. S3's own test event announces none. */
fun arrivedFiles(sqsBody: String): List<ArrivedFile> =
    notificationJson.decodeFromString(S3Notification.serializer(), sqsBody).Records.map {
        ArrivedFile(
            bucket = it.s3.bucket.name,
            key = java.net.URLDecoder.decode(it.s3.`object`.key, Charsets.UTF_8),
            arrivedAt = it.eventTime?.let { t -> Instant.parse(t).toEpochMilli() } ?: System.currentTimeMillis(),
        )
    }

/**
 * | Hears (a settlement file line) | Produces                    |
 * |--------------------------------|-----------------------------|
 * | `SETTLED`, ref p-7             | FUNDS_DEPOSITED             |
 * | `REJECTED`, ref p-7            | DEPOSIT_FAILED (`reason`)   |
 *
 * The event id is the file name plus the line's reference, so reprocessing the same file produces the same
 * events. `raw` is the line.
 */
fun settlementEvent(file: ArrivedFile, line: SettlementLine, payment: Payment): PaymentEvent {
    val fileName = file.key.substringAfterLast('/')
    val raw = JsonRaw(buildJsonObject {
        put("file", JsonPrimitive(file.key))
        put("line", JsonPrimitive(line.toCsv()))
    })
    return when (line.status) {
        SettlementLine.SETTLED ->
            FundsDeposited(payment, "$fileName:${line.reference}.funds-deposited", file.arrivedAt, payment.id, raw = raw)
        SettlementLine.REJECTED ->
            DepositFailed(payment, bankNetworkReason(line.code), "$fileName:${line.reference}.deposit-failed",
                file.arrivedAt, payment.id, raw = raw)
        else -> error("$fileName: unknown settlement status ${line.status} for ${line.reference}")
    }
}

/** Reads a file's content. */
fun interface FileReader {
    suspend fun read(file: ArrivedFile): String
}

class S3FileReader : FileReader {
    override suspend fun read(file: ArrivedFile): String {
        val client = AwsGlobalRegistry.s3ClientFactory().getClient("files")
        return client.getObject(GetObjectRequest { bucket = file.bucket; key = file.key }) { response ->
            response.body?.decodeToString() ?: ""
        }
    }
}

class FilesContainer(
    val files: FileReader,
    val lookup: PaymentLookup,
    val eventPublisher: EventPublisher,
) {
    companion object {
        fun build(): FilesContainer {
            installGatewayFaultManager()
            return FilesContainer(S3FileReader(), DynamoDbPaymentLookup.fromEnvironment(), AwsGlobalRegistry.eventPublisher())
        }
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.builder()
            .addPipeline(
                TranslatePipeline.builder()
                    .id("settlements")
                    .toEvents { uow ->
                        val file = uow.record as ArrivedFile
                        parseSettlementFile(files.read(file)).map { line -> settlementEvent(file, line, lookup.payment(line.reference)) }
                    }
                    .eventPublisher(eventPublisher)
                    .build()
            )
            .build()
    }
}

/** Settlement files (S3 → SQS) → our events. One unit of work per file. */
class FileListener(
    containerFactory: () -> FilesContainer = { FilesContainer.build() },
) : RequestHandler<SQSEvent, Void?> {
    private val logger = KotlinLogging.logger { }
    private val container by loggedLazy("FilesContainer", logger, containerFactory)

    override fun handleRequest(event: SQSEvent, context: Context): Void? {
        val files = event.records.flatMap { arrivedFiles(it.body) }
        logger.info { "settlement files: ${files.map { it.key }}" }
        runAssembler(container.assembler, files.map { UnitOfWork(record = it) }.asFlow())
        return null
    }
}

/** Default channel → the bank deposits we sent to the network. */
class RequestsContainer(table: String) {
    companion object {
        fun build() = RequestsContainer(System.getenv("REQUESTS_TABLE_NAME") ?: error("REQUESTS_TABLE_NAME is not configured"))
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(requestsPipeline("requests", table, depositsTo(Party.BANK)))
        .build()

    val kinesisAdapter = KinesisAdapter(eventCodec = PaymentsEventCodec)
}

class Listener(
    containerFactory: () -> RequestsContainer = { RequestsContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {
    private val container by loggedLazy("RequestsContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        runKinesis(container.assembler, container.kinesisAdapter, event)
        return null
    }
}
