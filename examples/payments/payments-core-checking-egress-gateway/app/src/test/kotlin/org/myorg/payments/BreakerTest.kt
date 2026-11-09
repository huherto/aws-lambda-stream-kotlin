package org.myorg.payments

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class InMemoryBreakerStore : BreakerStore {
    var state = BreakerState()
    override suspend fun read() = state
    override suspend fun write(next: BreakerState, expectedVersion: Long): Boolean {
        if (state.version != expectedVersion) return false
        state = next
        return true
    }
}

class BreakerTest {

    private val store = InMemoryBreakerStore()
    private var now = 1_000_000L
    private val breaker = Breaker(store, threshold = 3, openFor = 30.seconds, clock = { now })

    @Test
    fun `closed - calls go through and an isolated failure does not open it`() = runTest {
        // Act
        breaker.failed()
        breaker.failed()
        breaker.succeeded()
        breaker.failed()

        // Assert
        breaker.admit().shouldBeNull()
        store.state.state shouldBe BreakerState.CLOSED
    }

    @Test
    fun `three failures in a row open it, and nothing is admitted until the cool-down is over`() = runTest {
        // Act
        repeat(3) { breaker.failed() }

        // Assert
        store.state.state shouldBe BreakerState.OPEN
        store.state.opens shouldBe 1
        breaker.admit() shouldBe now + 30_000
        now += 29_999
        breaker.admit().shouldNotBeNull()
    }

    @Test
    fun `after the cool-down exactly one trial is admitted`() = runTest {
        // Arrange
        repeat(3) { breaker.failed() }
        now += 30_000

        // Act
        val first = breaker.admit()
        val second = breaker.admit()

        // Assert
        first.shouldBeNull()
        second.shouldNotBeNull()
        store.state.state shouldBe BreakerState.HALF_OPEN
    }

    @Test
    fun `a successful trial closes it and records when core checking came back`() = runTest {
        // Arrange
        repeat(3) { breaker.failed() }
        now += 30_000
        breaker.admit()

        // Act
        breaker.succeeded()

        // Assert
        store.state.state shouldBe BreakerState.CLOSED
        breaker.closedAt() shouldBe now
        breaker.admit().shouldBeNull()
    }

    @Test
    fun `a failed trial opens it again`() = runTest {
        // Arrange
        repeat(3) { breaker.failed() }
        now += 30_000
        breaker.admit()

        // Act
        breaker.failed()

        // Assert
        store.state.state shouldBe BreakerState.OPEN
        store.state.opens shouldBe 2
        breaker.admit() shouldBe now + 30_000
    }

    @Test
    fun `a lost trial is replaced after a cool-down`() = runTest {
        // Arrange: the trial's function died without reporting
        repeat(3) { breaker.failed() }
        now += 30_000
        breaker.admit().shouldBeNull()

        // Act / Assert
        breaker.admit().shouldNotBeNull()
        now += 30_001
        breaker.admit().shouldBeNull()
    }

    @Test
    fun `closed hours keep it open without any failure, overnight windows included`() = runTest {
        // Arrange
        val hours = ClosedHours.parse("22:00-06:00@America/New_York")!!
        fun at(h: Int, m: Int = 0) = ZonedDateTime.of(2026, 11, 9, h, m, 0, 0, java.time.ZoneId.of("America/New_York")).toInstant().toEpochMilli()

        // Act / Assert
        hours.isClosed(at(23)) shouldBe true
        hours.isClosed(at(3)) shouldBe true
        hours.isClosed(at(5, 59)) shouldBe true
        hours.isClosed(at(6)) shouldBe false
        hours.isClosed(at(12)) shouldBe false
        ClosedHours.parse("").shouldBeNull()

        now = at(23)
        Breaker(store, closedHours = hours, clock = { now }).admit().shouldNotBeNull()
        store.state.failures shouldBe 0
    }
}

class CalledOnceWithBreakerTest {

    private val coreChecking = FakeCoreChecking()
    private val claims = InMemoryClaims()
    private val store = InMemoryBreakerStore()
    private var now = 1_000_000L
    private val breaker = Breaker(store, threshold = 2, openFor = 30.seconds, clock = { now })
    private val calledOnce = CalledOnce(
        coreChecking, claims,
        grace = 300.seconds, clock = { now }, breaker = breaker, maxWait = 0.milliseconds,
    )

    @Test
    fun `while the breaker is open nothing is sent and the request waits on the stream`() = runTest {
        // Arrange
        repeat(2) { breaker.failed() }

        // Act / Assert
        shouldThrow<CoreCheckingUnavailable> { calledOnce.debit(Fixtures.payment) }
        coreChecking.calls.shouldBeEmpty()
        claims.claims.isEmpty() shouldBe true
    }

    @Test
    fun `refused connections open the breaker, and once it is back the request is sent once`() = runTest {
        // Arrange: core checking is down
        repeat(2) {
            coreChecking.failNext = CallNotSent("connection refused")
            shouldThrow<CallNotSent> { calledOnce.debit(Fixtures.payment) }
        }
        store.state.state shouldBe BreakerState.OPEN

        // Act: after the cool-down, the trial call goes through
        now += 30_000
        calledOnce.debit(Fixtures.payment)

        // Assert
        store.state.state shouldBe BreakerState.CLOSED
        coreChecking.calls shouldContainExactly listOf("debit p-7", "debit p-7", "debit p-7")
        claims.claims["p-7#PMT"]?.state shouldBe Claim.SENT
    }

    @Test
    fun `a request in doubt during a long outage gets its full grace period once core checking is back`() = runTest {
        // Arrange: the answer is lost, then core checking is down for 10 minutes
        coreChecking.failNext = java.net.SocketTimeoutException("timed out")
        calledOnce.debit(Fixtures.payment) shouldBe Outcome.IN_DOUBT
        breaker.failed()
        now += 600_000
        breaker.admit().shouldBeNull()
        breaker.succeeded()

        // Act: core checking is back but doesn't show the posting yet
        now += 60_000

        // Assert: still in doubt, not a fault, and still not called again
        calledOnce.debit(Fixtures.payment) shouldBe Outcome.IN_DOUBT
        coreChecking.calls shouldContainExactly listOf("debit p-7")
    }

    @Test
    fun `a request already sent never takes the breaker's trial, so the trial goes to a request that calls`() = runTest {
        // Arrange: one request was sent before core checking went down; the breaker opened and cooled down
        calledOnce.debit(Fixtures.payment)
        repeat(2) { breaker.failed() }
        now += 30_000

        // Act: the stream redelivers the sent request first, then a new one
        calledOnce.debit(Fixtures.payment)
        calledOnce.debit(Fixtures.payment.copy(id = "p-8"))

        // Assert: the sent one didn't consult the breaker; the new one made the trial call, which closed it
        coreChecking.calls shouldContainExactly listOf("debit p-7", "debit p-8")
        store.state.state shouldBe BreakerState.CLOSED
    }

    @Test
    fun `an inquiry core checking can't answer counts as a failure and the request waits`() = runTest {
        // Arrange: in doubt
        coreChecking.failNext = java.net.SocketTimeoutException("timed out")
        calledOnce.debit(Fixtures.payment) shouldBe Outcome.IN_DOUBT

        // Act: the inquiry fails too
        val failing = object : CoreChecking by coreChecking {
            override suspend fun posted(payment: Payment, txnType: String): Boolean = throw CallNotSent("refused")
        }
        val withFailingInquiry = CalledOnce(
            failing, claims, clock = { now }, breaker = breaker, maxWait = 0.milliseconds,
        )

        // Assert
        shouldThrow<CoreCheckingUnavailable> { withFailingInquiry.debit(Fixtures.payment) }
        store.state.state shouldBe BreakerState.OPEN
    }

    @Test
    fun `C-5 - the simulated core checking goes down for everyone, once per payment`() = runTest {
        // Arrange
        var downUntil = 0L
        val started = mutableSetOf<String>()
        val outage = object : SimulatedOutage {
            override suspend fun startOnce(paymentId: String, now: Long) {
                if (started.add(paymentId)) downUntil = now + 60_000
            }
            override suspend fun isDown(now: Long) = now < downUntil
        }
        val simulated = SimulatedCoreChecking({ _, _ -> }, delaySeconds = 2, outage = outage, clock = { now })
        val down = Fixtures.payment.copy(id = "p-5", from = Party(Party.CHECKING, Simulation.OUTAGE_ACCOUNT))

        // Act / Assert
        shouldThrow<CallNotSent> { simulated.debit(down) }
        shouldThrow<CallNotSent> { simulated.debit(Fixtures.payment) }
        shouldThrow<CallNotSent> { simulated.posted(Fixtures.payment, LedgerEntry.PAYMENT) }
        now += 60_000
        simulated.debit(down)
        simulated.debit(Fixtures.payment)
    }
}
