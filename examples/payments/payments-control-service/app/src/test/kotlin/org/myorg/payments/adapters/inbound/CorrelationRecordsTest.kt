package org.myorg.payments.adapters.inbound

import org.myorg.payments.*

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.aws.DynamodbRaw
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.core.JsonEvent
import io.kopipes.core.UnitOfWork
import io.kopipes.core.sinks.queryParams
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.myorg.payments.adapters.inbound.StreamRecords.correlationItem
import org.myorg.payments.adapters.inbound.StreamRecords.eventItem
import org.myorg.payments.adapters.inbound.StreamRecords.insert
import org.myorg.payments.adapters.inbound.StreamRecords.ttlRemove

class CorrelationRecordsTest {

    private val adapter = DynamodbAdapter()

    private fun uowOf(record: DynamodbEvent.DynamodbStreamRecord): UnitOfWork =
        runBlocking { adapter.fromDynamoDB(StreamRecords.dynamodbEvent(record)).first() }

    @Test
    fun `isCorrelationInsert accepts only inserts of the payment's own correlation items`() {
        // Arrange
        val correlation = uowOf(insert(correlationItem(Fixtures.fundsWithdrawn())))
        val collected = uowOf(insert(eventItem(Fixtures.fundsWithdrawn())))
        val deadline = uowOf(insert(correlationItem(Fixtures.depositRequested(), suffix = DEADLINE_SUFFIX, expire = true)))

        // Act / Assert
        isCorrelationInsert(correlation) shouldBe true
        isCorrelationInsert(collected) shouldBe false
        isCorrelationInsert(deadline) shouldBe false
    }

    @Test
    fun `isDeadlineExpiry accepts only the removal of an expiring deadline item`() {
        // Arrange
        val expired = uowOf(ttlRemove(correlationItem(Fixtures.depositRequested(), suffix = DEADLINE_SUFFIX, expire = true)))
        val notExpiring = uowOf(ttlRemove(correlationItem(Fixtures.depositRequested(), suffix = DEADLINE_SUFFIX)))
        val oldCorrelation = uowOf(ttlRemove(correlationItem(Fixtures.depositRequested())))
        val inserted = uowOf(insert(correlationItem(Fixtures.depositRequested(), suffix = DEADLINE_SUFFIX, expire = true)))

        // Act / Assert
        isDeadlineExpiry(expired) shouldBe true
        isDeadlineExpiry(notExpiring) shouldBe false
        isDeadlineExpiry(oldCorrelation) shouldBe false
        isDeadlineExpiry(inserted) shouldBe false
    }

    @Test
    fun `correlationNormalizer decodes the event and derives the emitted id from payment and pipeline`() {
        // Arrange
        val uow = uowOf(insert(correlationItem(Fixtures.fundsWithdrawn())))
        val normalize = correlationNormalizer(Rules.NEXT_STEP, PaymentsEventCodec, { it.new })

        // Act
        val normalized = normalize(uow)

        // Assert
        normalized.event shouldBe Fixtures.fundsWithdrawn()
        normalized.meta?.get("eventId") shouldBe "p-7.next-step"
        normalized.meta?.get("partitionKey") shouldBe "p-7"
        normalized.queryParams?.pk shouldBe "p-7"
        normalized.queryParams?.correlation shouldBe true
    }

    @Test
    fun `correlationNormalizer strips the deadline suffix and atDeadline dates the event at the deadline`() {
        // Arrange
        val ttl = 1730900700L
        val uow = uowOf(ttlRemove(correlationItem(Fixtures.depositRequested(), suffix = DEADLINE_SUFFIX, ttl = ttl, expire = true)))
        val normalize = correlationNormalizer(Rules.TIMEOUT, PaymentsEventCodec, { it.old }, ::atDeadline)

        // Act
        val normalized = normalize(uow)

        // Assert
        normalized.event?.timestamp shouldBe ttl * 1000
        normalized.meta?.get("eventId") shouldBe "p-7.timeout"
        normalized.meta?.get("suffix") shouldBe DEADLINE_SUFFIX
        normalized.queryParams?.pk shouldBe "p-7"
        (uow.event?.raw).shouldBeInstanceOf<DynamodbRaw>()
    }

    @Test
    fun `correlatedEvents decodes the generic JSON events the DynamoDB microstore returns`() {
        // Arrange
        val uow = UnitOfWork(correlated = listOf(JsonEvent(Fixtures.depositFailed().toString()), Fixtures.fundsRefunded()))

        // Act
        val events = correlatedEvents(uow)

        // Assert
        events.map { it.eventType() } shouldContainExactly listOf(PaymentsEvent.DEPOSIT_FAILED, PaymentsEvent.FUNDS_REFUNDED)
        events.first().shouldBeInstanceOf<DepositFailed>()
    }


}
