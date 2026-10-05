package io.kopipes.aws.testsupport

import com.amazonaws.services.lambda.runtime.ClientContext
import com.amazonaws.services.lambda.runtime.CognitoIdentity
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger

class TestContext(
    private val awsRequestId: String = "test-request-id",
    private val logGroupName: String = "test-log-group",
    private val logStreamName: String = "test-log-stream",
    private val functionName: String = "test-function",
    private val functionVersion: String = "1.0",
    private val invokedFunctionArn: String = "arn:aws:lambda:us-east-1:123456789012:function:test-function",
    private val memoryLimitInMB: Int = 512,
    private val remainingTimeInMillis: Int = 300000,
    private val logger: LambdaLogger = TestLogger()
) : Context {
    override fun getAwsRequestId(): String = awsRequestId
    override fun getLogGroupName(): String = logGroupName
    override fun getLogStreamName(): String = logStreamName
    override fun getFunctionName(): String = functionName
    override fun getFunctionVersion(): String = functionVersion
    override fun getInvokedFunctionArn(): String = invokedFunctionArn
    override fun getIdentity(): CognitoIdentity? = null
    override fun getClientContext(): ClientContext? = null
    override fun getRemainingTimeInMillis(): Int = remainingTimeInMillis
    override fun getMemoryLimitInMB(): Int = memoryLimitInMB
    override fun getLogger(): LambdaLogger = logger
}

class TestLogger : LambdaLogger {
    override fun log(message: String?) {
        println(message)
    }

    override fun log(message: ByteArray?) {
        message?.let { println(String(it)) }
    }
}
