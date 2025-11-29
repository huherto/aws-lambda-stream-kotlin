package org.myorg.sut.testsupport

import com.amazonaws.services.lambda.runtime.ClientContext
import com.amazonaws.services.lambda.runtime.CognitoIdentity
import com.amazonaws.services.lambda.runtime.Context

class TestContext(
    private val functionName: String = "test-function",
    private val functionVersion: String = "\$LATEST",
    private val memoryLimitInMB: Int = 512,
    private val awsRequestId: String = "test-request-id",
) : Context {

    private val logger = TestLogger()

    override fun getAwsRequestId(): String = awsRequestId
    override fun getLogGroupName(): String = "/aws/lambda/$functionName"
    override fun getLogStreamName(): String = "2025/01/01/[${functionVersion}]/abcdef"
    override fun getFunctionName(): String = functionName
    override fun getFunctionVersion(): String = functionVersion
    override fun getInvokedFunctionArn(): String = "arn:aws:lambda:<region>:<account-id>:function:$functionName"
    override fun getIdentity(): CognitoIdentity? = null
    override fun getClientContext(): ClientContext? = null
    override fun getRemainingTimeInMillis(): Int = 30_000
    override fun getMemoryLimitInMB(): Int = memoryLimitInMB
    override fun getLogger() = logger
}
