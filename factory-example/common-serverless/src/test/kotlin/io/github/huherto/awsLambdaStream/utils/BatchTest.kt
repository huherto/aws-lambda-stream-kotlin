package io.github.huherto.awsLambdaStream.utils

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList

class BatchTest : FunSpec({

    context("toBatchUow & unBatchUow") {

        test("should wrap multiple units of work into a batch and unwrap them applying outer properties") {
            // Arrange
            val inner1 = UnitOfWork(key = "Inner1")
            val inner2 = UnitOfWork(key = "Inner2")

            // Act
            val batched = toBatchUow(listOf(inner1, inner2))
            val outerWrapper = batched.copy(key = "OuterKey") // Outer properties replace inner when unwrapping
            val unbatched = unBatchUow(outerWrapper)

            // Assert
            batched.batch shouldBe listOf(inner1, inner2)

            unbatched shouldHaveSize 2
            unbatched[0].key shouldBe "OuterKey"
            unbatched[0].batch shouldBe null
            unbatched[1].key shouldBe "OuterKey"
            unbatched[1].batch shouldBe null
        }

        test("should return original unit of work as a singleton list if batch is null") {
            // Arrange
            val uow = UnitOfWork(key = "SingleItem")

            // Act
            val unbatched = unBatchUow(uow)

            // Assert
            unbatched shouldHaveSize 1
            unbatched.first() shouldBe uow
        }
    }

    context("group & toGroupUows") {

        test("should group items or pass them through directly based on PipelineRule") {
            // Arrange
            val uows = listOf(UnitOfWork(key = "1"), UnitOfWork(key = "2"))
            val activeRule = PipelineRule(group = true)
            val inactiveRule = PipelineRule(group = false)

            // Act
            val groupedResult = uows.asFlow().group(activeRule).toList()
            val ungroupedResult = uows.asFlow().group(inactiveRule).toList()

            // Assert
            ungroupedResult shouldHaveSize 2

            // By default (no event), it groups by a null partitionKey, merging everything into 1 batch.
            groupedResult shouldHaveSize 1
            groupedResult[0].batch?.shouldHaveSize(2)
        }

        test("toGroupUows should map a grouped map to a list of UnitOfWork batches") {
            // Arrange
            val groups = mapOf<String?, List<UnitOfWork>>(
                "groupA" to listOf(UnitOfWork(key = "1")),
                "groupB" to listOf(UnitOfWork(key = "2"), UnitOfWork(key = "3"))
            )

            // Act
            val result = toGroupUows(groups)

            // Assert
            result shouldHaveSize 2
            result[0].batch?.shouldHaveSize(1)
            result[1].batch?.shouldHaveSize(2)
        }
    }

    context("compact") {

        test("should compact items based on custom grouping/sorting, or pass through if rule is empty") {
            // Arrange
            val uows = listOf(
                UnitOfWork(key = "A"),
                UnitOfWork(key = "B"),
                UnitOfWork(key = "C")
            )
            val inactiveRule = PipelineRule(compact = null)
            val activeRule = PipelineRule(
                compact = CompactRule(
                    group = { "StaticGroup" },
                    sort = { a, b -> a.key.orEmpty().compareTo(b.key.orEmpty()) }
                )
            )

            // Act
            val compactedResult = uows.asFlow().compact(activeRule).toList()
            val uncompactedResult = uows.asFlow().compact(inactiveRule).toList()

            // Assert
            uncompactedResult shouldHaveSize 3

            compactedResult shouldHaveSize 1 // All 3 grouped into "StaticGroup"
            val compactedItem = compactedResult.first()
            compactedItem.key shouldBe "C" // 'C' sorts last, overriding outer fields
            compactedItem.batch?.shouldHaveSize(3) // Contains sorted A, B, C
        }
    }

    context("batchWithSize") {

        test("should batch items together depending on element count OR max aggregate size limits") {
            // Arrange
            val items = listOf(
                UnitOfWork(key = "A"),
                UnitOfWork(key = "B"),
                UnitOfWork(key = "C"),
                UnitOfWork(key = "D")
            )
            val batchByCountOptions = BatchSizeOptions(
                maxRequestSize = 100, // Large enough to ignore byte size limit
                batchSize = 2,
                getRequestEntry = { it.key },
                claimCheckBucketName = null
            )
            val batchBySizeOptions = BatchSizeOptions(
                maxRequestSize = 2, // 'A' (1 byte) + 'B' (1 byte) = 2 bytes max per chunk
                batchSize = 10,
                getRequestEntry = { it.key },
                claimCheckBucketName = null
            )

            // Act
            val resultByCount = items.asFlow().batchWithSize(batchByCountOptions).toList()
            val resultBySize = items.asFlow().batchWithSize(batchBySizeOptions).toList()

            // Assert
            resultByCount shouldHaveSize 2
            resultByCount[0].map { it.key } shouldBe listOf("A", "B")
            resultByCount[1].map { it.key } shouldBe listOf("C", "D")

            resultBySize shouldHaveSize 2
            resultBySize[0].map { it.key } shouldBe listOf("A", "B")
            resultBySize[1].map { it.key } shouldBe listOf("C", "D")
        }

        test("should bypass batching entirely and emit a singleton list if getRequestEntry evaluates to null") {
            // Arrange
            val items = listOf(
                UnitOfWork(key = null),
                UnitOfWork(key = "ValidEntry")
            )
            val options = BatchSizeOptions(
                maxRequestSize = 100,
                batchSize = 10,
                getRequestEntry = { it.key },
                claimCheckBucketName = null
            )

            // Act
            val result = items.asFlow().batchWithSize(options).toList()

            // Assert
            result shouldHaveSize 2
            result[0].map { it.key } shouldBe listOf(null)
            result[1].map { it.key } shouldBe listOf("ValidEntry")
        }

        test("should reject flow or attempt claim check when a single item size exceeds maxRequestSize") {
            // Arrange
            val items = listOf(UnitOfWork(key = "OversizedItem")) // String is 13 bytes
            val optionsWithoutBucket = BatchSizeOptions(
                maxRequestSize = 5,
                batchSize = 10,
                getRequestEntry = { it.key },
                claimCheckBucketName = null
            )
            val optionsWithBucket = optionsWithoutBucket.copy(
                claimCheckBucketName = "claimcheck-bucket"
            )

            // Act & Assert (Without Bucket -> Throws Exception)
            val exception = shouldThrow<Exception> {
                items.asFlow().batchWithSize(optionsWithoutBucket).toList()
            }
            exception.message shouldBe "Request size: 13, exceeded max: 5"

            // Act & Assert (With Bucket -> Intercepts `TODO()` from `toPutClaimcheckRequest` logic)
            shouldThrow<NotImplementedError> {
                items.asFlow().batchWithSize(optionsWithBucket).toList()
            }
        }
    }

    context("batchWithPayloadSizeOrCount") {

        test("should batch items together depending on element count OR max aggregate payload size limits") {
            // Arrange
            val items = listOf(
                UnitOfWork(key = "1"),
                UnitOfWork(key = "2"),
                UnitOfWork(key = "3")
            )
            val batchByCountOptions = PayloadSizeOptions(
                batchSize = 2,
                maxPayloadSize = 100,
                getPayload = { it.key }
            )
            val batchBySizeOptions = PayloadSizeOptions(
                batchSize = 10,
                maxPayloadSize = 2, // '1' + '2' = 2 bytes
                getPayload = { it.key }
            )

            // Act
            val resultByCount = items.asFlow().batchWithPayloadSizeOrCount(batchByCountOptions).toList()
            val resultBySize = items.asFlow().batchWithPayloadSizeOrCount(batchBySizeOptions).toList()

            // Assert
            resultByCount shouldHaveSize 2
            resultByCount[0].map { it.key } shouldBe listOf("1", "2")
            resultByCount[1].map { it.key } shouldBe listOf("3")

            resultBySize shouldHaveSize 2
            resultBySize[0].map { it.key } shouldBe listOf("1", "2")
            resultBySize[1].map { it.key } shouldBe listOf("3")
        }

        test("should bypass batching and emit a singleton list immediately if getPayload returns null") {
            // Arrange
            val items = listOf(
                UnitOfWork(key = null),
                UnitOfWork(key = "ValidPayload")
            )
            val options = PayloadSizeOptions(
                batchSize = 10,
                maxPayloadSize = 100,
                getPayload = { it.key }
            )

            // Act
            val result = items.asFlow().batchWithPayloadSizeOrCount(options).toList()

            // Assert
            result shouldHaveSize 2
            result[0].map { it.key } shouldBe listOf(null)
            result[1].map { it.key } shouldBe listOf("ValidPayload")
        }

        test("should throw an exception if a single isolated payload size exceeds maxPayloadSize") {
            // Arrange
            val items = listOf(UnitOfWork(key = "MassivePayload")) // String is 14 bytes
            val options = PayloadSizeOptions(
                batchSize = 10,
                maxPayloadSize = 10,
                getPayload = { it.key }
            )

            // Act & Assert
            val exception = shouldThrow<Exception> {
                items.asFlow().batchWithPayloadSizeOrCount(options).toList()
            }
            exception.message shouldBe "Payload size: 14, exceeded max: 10"
        }
    }
})