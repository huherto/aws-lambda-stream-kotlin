package io.github.huherto.awsLambdaStream.faults

import com.fasterxml.uuid.Generators
import io.github.huherto.awsLambdaStream.FaultException
import io.github.huherto.awsLambdaStream.UnitOfWork
import java.util.*

class FaultEventFactory(
    private val awsLambdaFunctionName: String = "undefined",
    private val unitOfWorkSnapshotter: UnitOfWorkSnapshotter = DefaultUnitOfWorkSnapshotter(),
    private val redactor: SnapshotRedactor = NoOpSnapshotRedactor,
    private val options: FaultSnapshotOptions = FaultSnapshotOptions()
) {
    private val uuidV1Generator = Generators.timeBasedGenerator()

    fun createFaultEvent(
        uow: UnitOfWork?,
        error: Throwable
    ): FaultEvent {
        val faultException = if (error is FaultException) error else FaultException(uow, error)

        val pipelineId = uow?.pipeline?.id ?: "undefined"

        return FaultEvent().apply {
            id = uuidV1Generator.generate().toString()
            partitionKey = UUID.randomUUID().toString()
            timestamp = System.currentTimeMillis()
            tags = mutableMapOf(
                "functionname" to awsLambdaFunctionName,
                "pipeline" to pipelineId
            )
            err = ErrorSnapshot(
                name = error.javaClass.simpleName,
                message = error.message,
                stackTrace = if (options.includeStackTrace) {
                    error.stackTrace.take(options.maxStackTraceFrames).map { it.toString() }
                } else null
            )
            this.uow = uow?.let {
                val snapshot = unitOfWorkSnapshotter.snapshot(it)
                redactor.redact(snapshot)
            }
            this.runtimeUow = uow
            this.faultException = faultException
        }
    }
}
