package org.myorg.payments.adapters.outbound

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.ConditionalCheckFailedException
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.kopipes.aws.utils.DynamoDbAttributeValueMapReader
import io.kopipes.core.utils.AttributeValueMapReader
import org.myorg.payments.Account
import org.myorg.payments.Party
import org.myorg.payments.Payment
import org.myorg.payments.application.PaymentKey
import org.myorg.payments.application.PaymentsPage
import org.myorg.payments.application.PaymentsStore
import org.myorg.payments.application.SubmitOutcome
import org.myorg.payments.domain.PaymentRecord
import org.myorg.payments.domain.ValidRequest

/**
 * The BFF's one table. Two kinds of items:
 * - `pk = <payment id>, sk = PAYMENT`: the payment as accepted, plus `at_<TYPE>` / `reason_<TYPE>` for each heard event.
 * - `pk = <account number>, sk = ACCOUNT`: the latest known state of an account, from ACCOUNT_CHANGED.
 * The sparse index `byCustomer` (`customerId`, `submittedAt`) holds only accepted payments.
 */
object Items {
    const val PAYMENT = "PAYMENT"
    const val ACCOUNT = "ACCOUNT"
    const val BY_CUSTOMER = "byCustomer"

    const val HEARD_PREFIX = "at_"
    const val REASON_PREFIX = "reason_"

    const val CUSTOMER_ID = "customerId"
    const val SUBMITTED_AT = "submittedAt"
    const val REQUEST_HASH = "requestHash"

    fun key(pk: String, sk: String): Map<String, AttributeValue> =
        mapOf("pk" to AttributeValue.S(pk), "sk" to AttributeValue.S(sk))
}

/**
 * Reads a payment item. `null` until the payment has been accepted: the listener may have recorded events
 * for an id that no `PUT` has created yet (for instance during a replay), and such an item is not a payment.
 */
fun paymentRecordOf(names: Set<String>, item: AttributeValueMapReader): PaymentRecord? {
    val requestHash = item.getS(Items.REQUEST_HASH) ?: return null
    val payment = Payment(
        id = item.getS("pk") ?: return null,
        customerId = item.getS(Items.CUSTOMER_ID) ?: return null,
        from = Party(item.getS("fromKind") ?: return null, item.getS("fromAccount") ?: return null),
        to = Party(item.getS("toKind") ?: return null, item.getS("toAccount") ?: return null),
        amount = item.getS("amount") ?: return null,
        currency = item.getS("currency") ?: return null,
        submittedAt = item.getLong(Items.SUBMITTED_AT) ?: return null,
    )
    val heard = names.filter { it.startsWith(Items.HEARD_PREFIX) }
        .mapNotNull { name -> item.getLong(name)?.let { name.removePrefix(Items.HEARD_PREFIX) to it } }
        .toMap()
    val reasons = names.filter { it.startsWith(Items.REASON_PREFIX) }
        .mapNotNull { name -> item.getS(name)?.let { name.removePrefix(Items.REASON_PREFIX) to it } }
        .toMap()
    return PaymentRecord(payment = payment, requestHash = requestHash, heard = heard, reasons = reasons)
}

fun accountOf(item: AttributeValueMapReader): Account? {
    return Account(
        number = item.getS("pk") ?: return null,
        kind = item.getS("kind") ?: return null,
        customerId = item.getS(Items.CUSTOMER_ID) ?: return null,
        status = item.getS("status") ?: return null,
    )
}

/** The [PaymentsStore] port on the BFF's DynamoDB table. */
class DynamoDbPaymentsStore(
    private val client: DynamoDbClient,
    private val tableName: String,
) : PaymentsStore {

    override suspend fun findAccount(number: String): Account? {
        val item = client.getItem(GetItemRequest {
            tableName = this@DynamoDbPaymentsStore.tableName
            key = Items.key(number, Items.ACCOUNT)
            consistentRead = true
        }).item ?: return null
        return accountOf(DynamoDbAttributeValueMapReader(item))
    }

    override suspend fun findPayment(id: String): PaymentRecord? {
        val item = client.getItem(GetItemRequest {
            tableName = this@DynamoDbPaymentsStore.tableName
            key = Items.key(id, Items.PAYMENT)
            consistentRead = true
        }).item ?: return null
        return paymentRecordOf(item.keys, DynamoDbAttributeValueMapReader(item))
    }

    override suspend fun submit(request: ValidRequest, submittedAt: Long): SubmitOutcome {
        return try {
            client.updateItem(submitRequest(tableName, request, submittedAt))
            SubmitOutcome.Accepted(PaymentRecord(request.toPayment(submittedAt), request.hash))
        } catch (e: ConditionalCheckFailedException) {
            SubmitOutcome.AlreadyExists(findPayment(request.id))
        }
    }

    override suspend fun listByCustomer(customerId: String, after: PaymentKey?, limit: Int): PaymentsPage {
        val response = client.query(QueryRequest {
            tableName = this@DynamoDbPaymentsStore.tableName
            indexName = Items.BY_CUSTOMER
            keyConditionExpression = "#c = :c"
            expressionAttributeNames = mapOf("#c" to Items.CUSTOMER_ID)
            expressionAttributeValues = mapOf(":c" to AttributeValue.S(customerId))
            scanIndexForward = false
            this.limit = limit
            exclusiveStartKey = after?.let {
                Items.key(it.id, Items.PAYMENT) + mapOf(
                    Items.CUSTOMER_ID to AttributeValue.S(customerId),
                    Items.SUBMITTED_AT to AttributeValue.N(it.submittedAt.toString()),
                )
            }
        })
        val records = response.items.orEmpty().mapNotNull { paymentRecordOf(it.keys, DynamoDbAttributeValueMapReader(it)) }
        return PaymentsPage(records = records, more = !response.lastEvaluatedKey.isNullOrEmpty())
    }
}

/**
 * Accepts a payment once. An update, not a put: the listener may already have recorded events for this id.
 * The condition makes a repeated `PUT` write nothing, so `PAYMENT_SUBMITTED` is published once.
 */
fun submitRequest(table: String, request: ValidRequest, submittedAt: Long): UpdateItemRequest {
    val values = mapOf(
        Items.CUSTOMER_ID to AttributeValue.S(request.customerId),
        "fromKind" to AttributeValue.S(request.from.kind),
        "fromAccount" to AttributeValue.S(request.from.account),
        "toKind" to AttributeValue.S(request.to.kind),
        "toAccount" to AttributeValue.S(request.to.account),
        "amount" to AttributeValue.S(request.amount),
        "currency" to AttributeValue.S(request.currency),
        Items.SUBMITTED_AT to AttributeValue.N(submittedAt.toString()),
        Items.REQUEST_HASH to AttributeValue.S(request.hash),
        "discriminator" to AttributeValue.S(Items.PAYMENT),
    )
    val names = values.keys.mapIndexed { i, name -> "#a$i" to name }.toMap()
    val placeholders = values.keys.mapIndexed { i, _ -> ":v$i" }
    return UpdateItemRequest {
        tableName = table
        key = Items.key(request.id, Items.PAYMENT)
        updateExpression = "SET " + names.keys.zip(placeholders).joinToString(", ") { (n, v) -> "$n = $v" }
        conditionExpression = "attribute_not_exists(#hash)"
        expressionAttributeNames = names + ("#hash" to Items.REQUEST_HASH)
        expressionAttributeValues = placeholders.zip(values.values).toMap()
    }
}
