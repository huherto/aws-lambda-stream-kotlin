import aws.smithy.kotlin.runtime.SdkBaseException
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FailureEvent
import io.github.huherto.awsLambdaStream.FailureException
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

val theFaults = ConcurrentLinkedQueue<FailureEvent>()

val envConfig = EnvironmentConfig()

val logger = mu.KotlinLogging.logger { }

inline fun <T, R> T.faulty(uom: UnitOfWork, block: T.() -> R): R? {
    return try {
        block()
    } catch (e: Throwable) {
        val failureException = FailureException(uom, e)
        redirectFailure(failureException)
        null
    }
}

private fun isRetriableException(exception: FailureException) : Boolean{
    if (!envConfig.streamRetryEnabled()) return false
    if (exception.cause is SdkBaseException) {
        return (exception.cause as SdkBaseException).sdkErrorMetadata.isRetryable
    }
    return false
}

val published =  ConcurrentLinkedQueue<FailureEvent>()

fun publish(fault: FailureEvent) {
    logger.info { "PipelineAssembler.publish: fault=$fault" }
    published.add(fault)
}

fun redirectFailure(ex: FailureException) {
    if (!isRetriableException(ex)) {
        val functionName = envConfig.awsLambdaFunctionName()?: "undefined"
        val failureEvent = FailureEvent().apply {
            id = UUID.randomUUID().toString()
            partitionKey = UUID.randomUUID().toString()
            timestamp = System.currentTimeMillis()
            tags = mutableMapOf(
                "functionname" to functionName
            )
            failureException = ex
        }
        theFaults.add(failureEvent)
    }
    else {
        throw ex
    }
}

suspend fun flushFaults() {

    val count = flow {
        while (true) {
            val fault = theFaults.poll()
            if (fault == null) {
                break
            }
            emit(fault)
        }
    }
        .buffer()
        .onEach { fault -> publish(fault)}
        .count()

    logger.info { "PipelineAssembler.flushFaults: count=$count" }
}
