package org.myorg.payments.adapters.inbound

import org.myorg.payments.*

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kopipes.core.sinks.saveOptions
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.myorg.payments.adapters.inbound.StreamRecords.correlationItem
import org.myorg.payments.adapters.inbound.StreamRecords.dynamodbEvent
import org.myorg.payments.adapters.inbound.StreamRecords.eventItem
import org.myorg.payments.adapters.inbound.StreamRecords.insert
import org.myorg.payments.adapters.inbound.StreamRecords.ttlRemove

class TriggerContainerTest {

    private lateinit var publisher: EventPublisherInMemory
    private lateinit var microstore: EventsMicrostoreInMemory
    private lateinit var trigger: Trigger

    private val deadlineSeconds = 120L

    @BeforeEach
    fun setUp() {
        publisher = EventPublisherInMemory()
        val faultManager = FaultManager(publisher, skipErrorLogging = true)
        GlobalRegistry.setFaultManager(faultManager)
        microstore = EventsMicrostoreInMemory(faultManager)
        val container = TriggerContainer(
            eventsMicrostore = microstore,
            eventPublisher = publisher,
            deadlineSeconds = deadlineSeconds,
        )
        trigger = Trigger { container }
    }

    @AfterEach
    fun tearDown() {
        AwsGlobalRegistry.reset()
    }

    private fun handle(vararg records: DynamodbEvent.DynamodbStreamRecord) {
        trigger.handleRequest(dynamodbEvent(*records), TestContext())
    }

    private fun published(): List<PaymentsEvent> = publisher.uows().mapNotNull { it.event as? PaymentsEvent }

    // -------------------------------------------------------------------------------------------- simple rules

    @Test
    fun `start - PAYMENT_SUBMITTED emits WITHDRAWAL_REQUESTED with a deterministic id`() {
        // Arrange
        val submitted = Fixtures.paymentSubmitted()

        // Act
        handle(insert(correlationItem(submitted)))

        // Assert
        val events = published()
        events shouldHaveSize 1
        val requested = events.single().shouldBeInstanceOf<WithdrawalRequested>()
        requested.id shouldBe "p-7.start"
        requested.partitionKey shouldBe "p-7"
        requested.timestamp shouldBe submitted.timestamp
        requested.payment shouldBe Fixtures.payment
        requested.triggers?.single()?.id shouldBe submitted.id
    }

    @Test
    fun `a rule that fires again emits the same event with the same id`() {
        // Arrange
        val submitted = Fixtures.paymentSubmitted()

        // Act
        handle(insert(correlationItem(submitted)))
        handle(insert(correlationItem(submitted)))

        // Assert
        published().map { it.id } shouldContainExactly listOf("p-7.start", "p-7.start")
    }

    @Test
    fun `next step - FUNDS_WITHDRAWN emits DEPOSIT_REQUESTED`() {
        // Act
        handle(insert(correlationItem(Fixtures.fundsWithdrawn())))

        // Assert
        val event = published().single().shouldBeInstanceOf<DepositRequested>()
        event.id shouldBe "p-7.next-step"
    }

    @Test
    fun `done - FUNDS_DEPOSITED emits PAYMENT_COMPLETED`() {
        // Act
        handle(insert(correlationItem(Fixtures.fundsDeposited())))

        // Assert
        val event = published().single().shouldBeInstanceOf<PaymentCompleted>()
        event.id shouldBe "p-7.done"
    }

    @Test
    fun `nothing to undo - WITHDRAWAL_FAILED emits PAYMENT_FAILED with the reason copied`() {
        // Act
        handle(insert(correlationItem(Fixtures.withdrawalFailed(reason = Reasons.INSUFFICIENT_FUNDS))))

        // Assert
        val event = published().single().shouldBeInstanceOf<PaymentFailed>()
        event.id shouldBe "p-7.nothing-to-undo"
        event.reason shouldBe Reasons.INSUFFICIENT_FUNDS
    }

    @Test
    fun `compensate - DEPOSIT_FAILED emits REFUND_REQUESTED`() {
        // Act
        handle(insert(correlationItem(Fixtures.depositFailed())))

        // Assert
        val event = published().single().shouldBeInstanceOf<RefundRequested>()
        event.id shouldBe "p-7.compensate"
    }

    @Test
    fun `events the service emits itself trigger no rule`() {
        // Act
        handle(
            insert(correlationItem(Fixtures.withdrawalRequested())),
            insert(correlationItem(Fixtures.refundRequested())),
            insert(correlationItem(Fixtures.paymentCompleted())),
            insert(correlationItem(Fixtures.paymentExpired())),
        )

        // Assert
        published().shouldBeEmpty()
    }

    @Test
    fun `collected events are correlated but trigger no rule by themselves`() {
        // Act
        handle(insert(eventItem(Fixtures.paymentSubmitted())))

        // Assert
        published().shouldBeEmpty()
        val correlation = microstore.getStored()["p-7"].shouldNotBeNull().single()
        correlation.saveOptions?.discriminator shouldBe "CORREL"
        correlation.saveOptions?.sk shouldBe "p-7.submitted"
    }

    // ------------------------------------------------------------------------------------------------- undone

    @Test
    fun `undone - FUNDS_REFUNDED emits PAYMENT_FAILED with the reason of the deposit failure`() {
        // Arrange
        handle(insert(eventItem(Fixtures.depositFailed(reason = Reasons.ACCOUNT_CLOSED))))
        publisher.reset()

        // Act
        handle(insert(correlationItem(Fixtures.fundsRefunded())))

        // Assert
        val event = published().single().shouldBeInstanceOf<PaymentFailed>()
        event.id shouldBe "p-7.undone"
        event.reason shouldBe Reasons.ACCOUNT_CLOSED
    }

    @Test
    fun `undone - a refund without a deposit failure can't be decided and becomes a fault`() {
        // Act
        handle(insert(correlationItem(Fixtures.fundsRefunded())))

        // Assert
        published().shouldBeEmpty()
        publisher.faults() shouldHaveSize 1
    }

    // ------------------------------------------------------------------------------------------------ timeout

    @Test
    fun `DEPOSIT_REQUESTED starts the clock - a deadline item that expires at the deadline`() {
        // Arrange
        val requested = Fixtures.depositRequested()

        // Act
        handle(insert(eventItem(requested)))

        // Assert
        val deadline = microstore.getStored()["p-7$DEADLINE_SUFFIX"].shouldNotBeNull().single().saveOptions.shouldNotBeNull()
        deadline.discriminator shouldBe "CORREL"
        deadline.expire shouldBe true
        deadline.suffix shouldBe DEADLINE_SUFFIX
        deadline.ttl shouldBe requested.timestamp!! / 1000 + deadlineSeconds
    }

    @Test
    fun `timeout - the deadline passes with no deposit outcome and PAYMENT_EXPIRED is emitted at the deadline`() {
        // Arrange
        val requested = Fixtures.depositRequested()
        val deadline = requested.timestamp!! / 1000 + deadlineSeconds
        handle(insert(eventItem(requested)))
        publisher.reset()

        // Act
        handle(ttlRemove(correlationItem(requested, suffix = DEADLINE_SUFFIX, ttl = deadline, expire = true)))

        // Assert
        val event = published().single().shouldBeInstanceOf<PaymentExpired>()
        event.id shouldBe "p-7.timeout"
        event.partitionKey shouldBe "p-7"
        event.timestamp shouldBe deadline * 1000
        event.payment shouldBe Fixtures.payment
    }

    @Test
    fun `timeout - a deposit heard before the deadline means no PAYMENT_EXPIRED`() {
        // Arrange
        val requested = Fixtures.depositRequested()
        handle(insert(eventItem(requested)), insert(eventItem(Fixtures.fundsDeposited())))
        publisher.reset()

        // Act
        handle(ttlRemove(correlationItem(requested, suffix = DEADLINE_SUFFIX, expire = true)))

        // Assert
        published().shouldBeEmpty()
    }

    @Test
    fun `timeout - a deposit failure heard before the deadline means no PAYMENT_EXPIRED`() {
        // Arrange
        val requested = Fixtures.depositRequested()
        handle(insert(eventItem(requested)), insert(eventItem(Fixtures.depositFailed())))
        publisher.reset()

        // Act
        handle(ttlRemove(correlationItem(requested, suffix = DEADLINE_SUFFIX, expire = true)))

        // Assert
        published().shouldBeEmpty()
    }

    @Test
    fun `items deleted by the TTL that are not deadlines are ignored`() {
        // Act
        handle(ttlRemove(correlationItem(Fixtures.depositRequested())))

        // Assert
        published().shouldBeEmpty()
    }

    @Test
    fun `late deposit - after PAYMENT_EXPIRED a FUNDS_DEPOSITED still completes the payment`() {
        // Arrange
        handle(insert(eventItem(Fixtures.depositRequested())), insert(eventItem(Fixtures.paymentExpired())))
        publisher.reset()

        // Act
        handle(insert(correlationItem(Fixtures.fundsDeposited())))

        // Assert
        published().single().shouldBeInstanceOf<PaymentCompleted>()
    }

    @Test
    fun `late failure - after PAYMENT_EXPIRED a DEPOSIT_FAILED still triggers the refund`() {
        // Arrange
        handle(insert(eventItem(Fixtures.depositRequested())), insert(eventItem(Fixtures.paymentExpired())))
        publisher.reset()

        // Act
        handle(insert(correlationItem(Fixtures.depositFailed())))

        // Assert
        published().single().shouldBeInstanceOf<RefundRequested>()
    }
}
