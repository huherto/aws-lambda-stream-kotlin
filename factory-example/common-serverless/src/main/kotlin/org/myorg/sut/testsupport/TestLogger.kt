package org.myorg.sut.testsupport

import com.amazonaws.services.lambda.runtime.LambdaLogger
import java.nio.charset.StandardCharsets

class TestLogger : LambdaLogger {
    private val buffer = StringBuilder()

    override fun log(message: String?) {
        buffer.append(message ?: "").append('\n')
    }

    override fun log(message: ByteArray?) {
        buffer.append(String(message ?: ByteArray(0), StandardCharsets.UTF_8)).append('\n')
    }

    fun content(): String = buffer.toString()
}
