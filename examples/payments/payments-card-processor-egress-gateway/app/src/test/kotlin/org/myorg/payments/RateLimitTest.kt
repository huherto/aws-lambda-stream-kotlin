package org.myorg.payments

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/** A per-second budget in memory, like the DynamoDB one. */
class InMemoryRateStore : RateStore {
    val used = mutableMapOf<Long, Int>()
    override suspend fun take(epochSecond: Long, limit: Int): Boolean {
        val n = used.getOrDefault(epochSecond, 0)
        if (n >= limit) return false
        used[epochSecond] = n + 1
        return true
    }
}

class RateLimitTest {

    private var now = 1_000_000_000L
    private val sleeps = mutableListOf<Long>()
    private val sleep: suspend (Long) -> Unit = { ms -> sleeps += ms; now += ms }
    private val store = InMemoryRateStore()
    private val bucket = TokenBucket(store, perSecond = 3, clock = { now }, sleep = sleep)

    @Test
    fun `calls within the budget go straight through`() = runTest {
        // Act
        repeat(3) { bucket.acquire() }

        // Assert
        sleeps shouldBe emptyList()
        store.used shouldBe mapOf(1_000_000L to 3)
    }

    @Test
    fun `beyond the budget a call waits for the next second, so no second ever exceeds it`() = runTest {
        // Arrange
        now += 250

        // Act
        repeat(7) { bucket.acquire() }

        // Assert
        sleeps shouldContainExactly listOf(750L, 1000L)
        store.used.values.toList() shouldContainExactly listOf(3, 3, 1)
    }

    @Test
    fun `the budget is shared - a second instance waits when the first used it up`() = runTest {
        // Arrange
        val other = TokenBucket(store, perSecond = 3, clock = { now }, sleep = sleep)
        repeat(3) { bucket.acquire() }

        // Act
        other.acquire()

        // Assert
        sleeps shouldContainExactly listOf(1000L)
    }

    @Test
    fun `a 429 is a safety net - wait as told and try the same credit again`() = runTest {
        // Arrange
        val calls = mutableListOf<String>()
        var rejections = 1
        val processor = CardProcessor { payment ->
            calls += payment.id
            if (rejections-- > 0) throw RateLimited(retryAfterMillis = 400)
        }
        val paced = PacedCardProcessor(processor, bucket, sleep = sleep)

        // Act
        paced.credit(Fixtures.payment)

        // Assert
        calls shouldContainExactly listOf("p-7", "p-7")
        sleeps shouldContainExactly listOf(400L)
    }

    @Test
    fun `repeated 429s hand the request back to the stream`() = runTest {
        // Arrange
        val paced = PacedCardProcessor({ throw RateLimited(retryAfterMillis = 100) }, bucket, maxAttempts = 3, sleep = sleep)

        // Act / Assert
        shouldThrow<RateLimited> { paced.credit(Fixtures.payment) }
        sleeps shouldContainExactly listOf(100L, 100L)
    }

    @Test
    fun `the simulated processor answers 429 above its rate, and counts it`() = runTest {
        // Arrange
        var counted = 0
        val limit = SimulatedRateLimit { at -> if (!store.take(at / 1000, 2)) { counted++; throw RateLimited(1000 - at % 1000) } }
        val simulated = SimulatedCardProcessor({ _, _ -> }, delaySeconds = 5, clock = { now }, limit = limit)

        // Act
        simulated.credit(Fixtures.payment)
        simulated.credit(Fixtures.payment)

        // Assert
        shouldThrow<RateLimited> { simulated.credit(Fixtures.payment) }
        counted shouldBe 1
    }

    @Test
    fun `paced at the processor's own rate, a burst gets no 429 at all`() = runTest {
        // Arrange: the processor allows 3 a second and checks its own budget, in its own store
        val processorStore = InMemoryRateStore()
        var tooMany = 0
        val processor = SimulatedCardProcessor(
            { _, _ -> }, delaySeconds = 5, clock = { now },
            limit = { at -> if (!processorStore.take(at / 1000, 3)) { tooMany++; throw RateLimited(1000 - at % 1000) } },
        )
        val paced = PacedCardProcessor(processor, bucket, sleep = sleep)

        // Act
        repeat(10) { n -> paced.credit(Fixtures.payment.copy(id = "p-$n")) }

        // Assert
        tooMany shouldBe 0
        processorStore.used.values.all { it <= 3 } shouldBe true
    }
}
