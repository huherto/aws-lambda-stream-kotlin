package io.github.huherto.awsLambdaStream.utils

import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList

// -- Placeholders for missing functions from '../sinks/claimcheck' --
fun toPutClaimcheckRequest(detail: Any, claimCheckBucketName: String): Any = TODO()
fun toClaimcheckEvent(detail: Any, claimCheckBucketName: String): Any = TODO()

// Used after batch steps
fun toBatchUow(batch: List<UnitOfWork>): UnitOfWork {
    return UnitOfWork(batch = batch)
}

// Use with flatMap/flatten
fun unBatchUow(uow: UnitOfWork): List<UnitOfWork> {
    val batch = uow.batch ?: return listOf(uow)
    val outerMinusBatch = uow.copy(batch = null)

    return batch.map { inner ->
        // Simulating the `{ ...inner, ...outerMinusBatch }` JS spread logic
        // Outer properties override inner ones if they are present.
        inner.copy(
            pipeline = outerMinusBatch.pipeline ?: inner.pipeline,
            record = outerMinusBatch.record ?: inner.record,
            event = outerMinusBatch.event ?: inner.event,
            key = outerMinusBatch.key ?: inner.key,
            sequenceNumber = outerMinusBatch.sequenceNumber ?: inner.sequenceNumber,
            shardId = outerMinusBatch.shardId ?: inner.shardId,
            timestamp = outerMinusBatch.timestamp ?: inner.timestamp,
            putRequest = outerMinusBatch.putRequest ?: inner.putRequest,
            putResponse = outerMinusBatch.putResponse ?: inner.putResponse,
            meta = outerMinusBatch.meta ?: inner.meta,
            queryRequest = outerMinusBatch.queryRequest ?: inner.queryRequest,
            triggers = outerMinusBatch.triggers ?: inner.triggers,
            correlated = outerMinusBatch.correlated ?: inner.correlated,
            batch = null
        )
    }
}

// Support models for rule configurations
data class PipelineRule(
    val group: Boolean = false,
    val compact: CompactRule? = null
)

data class CompactRule(
    val group: ((UnitOfWork) -> String?)? = null,
    val sort: ((UnitOfWork, UnitOfWork) -> Int)? = null
)

suspend fun Flow<UnitOfWork>.group(rule: PipelineRule): Flow<UnitOfWork> = flow {
    if (!rule.group) {
        emitAll(this@group)
        return@flow
    }

    // highland `.group` consumes the stream to group items
    val grouped = this@group.toList().groupBy { it.event?.partitionKey }
    val groupedUows = toGroupUows(grouped)
    
    groupedUows.forEach { emit(it) }
}

fun toGroupUows(groups: Map<String?, List<UnitOfWork>>): List<UnitOfWork> {
    return groups.values.map { UnitOfWork(batch = it) }
}

fun Flow<UnitOfWork>.compact(rule: CompactRule?): Flow<UnitOfWork> = flow {
    if (rule == null) {
        emitAll(this@compact)
        return@flow
    }

    val grouper = rule.group ?: { it.event?.partitionKey }
    val sorter = rule.sort ?: { lh, rh ->
        val lhTs = lh.event?.timestamp ?: 0L
        val rhTs = rh.event?.timestamp ?: 0L
        lhTs.compareTo(rhTs)
    }

    val grouped = this@compact.toList().groupBy(grouper)

    val compacted = grouped.values.map { groupBatch ->
        val sortedBatch = groupBatch.sortedWith(Comparator { a, b -> sorter(a, b) })
        val last = sortedBatch.last()

        last.copy(batch = sortedBatch)
    }

    compacted.forEach { emit(it) }
}


// Configuration classes for Batching Operations
data class BatchSizeOptions(
    val maxRequestSize: Int,
    val batchSize: Int,
    val getRequestEntry: (UnitOfWork) -> Any?,
    val setRequestEntryDetail: (UnitOfWork, Any) -> Unit = { _, _ -> },
    val setPutClaimcheckRequest: (UnitOfWork, Any) -> Unit = { _, _ -> },
    val claimCheckBucketName: String? = System.getenv("CLAIMCHECK_BUCKET_NAME"),
    val metricsEnabled: Boolean = false
)

/**
 * Batch EB request entries by size to avoid writing a batch that's too large to EB.
 */
fun Flow<UnitOfWork>.batchWithSize(opt: BatchSizeOptions): Flow<List<UnitOfWork>> = flow {
    val batched = mutableListOf<UnitOfWork>()
    val sizes = mutableListOf<Int>()

    try {
        collect { x ->
            val entry = opt.getRequestEntry(x)
            if (entry == null) {
                emit(listOf(x))
            } else {
                var size = calculateByteSize(entry)

                if (size > opt.maxRequestSize) {
                    logMetrics(listOf(x), listOf(size), opt.metricsEnabled)
                    if (opt.claimCheckBucketName != null) {
                        // Setup claim check
                        // You will need to implement detail extraction appropriately depending on the underlying object shape.
                        val claimcheckReq = toPutClaimcheckRequest(entry, opt.claimCheckBucketName)
                        opt.setPutClaimcheckRequest(x, claimcheckReq)

                        val claimcheckEvent = toClaimcheckEvent(entry, opt.claimCheckBucketName)
                        opt.setRequestEntryDetail(x, claimcheckEvent)
                        
                        size = calculateByteSize(opt.getRequestEntry(x))
                    } else {
                        throw Exception("Request size: $size, exceeded max: ${opt.maxRequestSize}")
                    }
                }

                val totalSize = sizes.sum() + size

                if (totalSize <= opt.maxRequestSize && batched.size + 1 <= opt.batchSize) {
                    batched.add(x)
                    sizes.add(size)
                } else {
                    logMetrics(batched, sizes, opt.metricsEnabled)
                    emit(batched.toList())
                    
                    batched.clear()
                    sizes.clear()
                    batched.add(x)
                    sizes.add(size)
                }
            }
        }
    } finally {
        if (batched.isNotEmpty()) {
            logMetrics(batched, sizes, opt.metricsEnabled)
            emit(batched.toList())
        }
    }
}

data class PayloadSizeOptions(
    val batchSize: Int,
    val maxPayloadSize: Int,
    val getPayload: (UnitOfWork) -> Any?,
    val metricsEnabled: Boolean = false
)

/**
 * Batches by aggregate payload size with a cap on payload count.
 */
fun Flow<UnitOfWork>.batchWithPayloadSizeOrCount(opt: PayloadSizeOptions): Flow<List<UnitOfWork>> = flow {
    val batched = mutableListOf<UnitOfWork>()
    val sizes = mutableListOf<Int>()

    try {
        collect { x ->
            val payload = opt.getPayload(x)
            if (payload == null) {
                emit(listOf(x))
            } else {
                val size = calculateByteSize(payload)
                if (size > opt.maxPayloadSize) {
                    logMetrics(listOf(x), listOf(size), opt.metricsEnabled)
                    throw Exception("Payload size: $size, exceeded max: ${opt.maxPayloadSize}")
                }

                val totalSize = sizes.sum() + size
                if (totalSize <= opt.maxPayloadSize && batched.size + 1 <= opt.batchSize) {
                    batched.add(x)
                    sizes.add(size)
                } else {
                    logMetrics(batched, sizes, opt.metricsEnabled)
                    emit(batched.toList())
                    
                    batched.clear()
                    sizes.clear()
                    batched.add(x)
                    sizes.add(size)
                }
            }
        }
    } finally {
        if (batched.isNotEmpty()) {
            logMetrics(batched, sizes, opt.metricsEnabled)
            emit(batched.toList())
        }
    }
}

// --- Helper Functions ---

private fun logMetrics(batch: List<UnitOfWork>, sizes: List<Int>, metricsEnabled: Boolean) {
    if (metricsEnabled && batch.isNotEmpty()) {
        // Translate your metrics logging logic here. E.g.,
        // println("Metrics logged: size=${batch.size}")
    }
}

/**
 * Simulates Buffer.byteLength(JSON.stringify(object)).
 * Replace with your actual JSON serialization logic (e.g., Jackson ObjectMapper or kotlinx.serialization).
 */
private fun calculateByteSize(obj: Any?): Int {
    if (obj == null) return 0
    // Example: return objectMapper.writeValueAsBytes(obj).size
    return obj.toString().toByteArray(Charsets.UTF_8).size 
}