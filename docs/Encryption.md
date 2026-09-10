# Envelope Encryption

The framework provides built-in support for **Envelope Encryption**, allowing you to protect sensitive data at the field level within your events and DynamoDB items. It uses AWS Key Management Service (KMS) to manage data keys and AES-GCM for symmetric data encryption.

## Overview

Envelope encryption is a practice where you encrypt your data with a data key, and then encrypt the data key with a master key managed by a service like AWS KMS.

In this framework:
1. A unique **Data Encryption Key (DEK)** is generated for each event or batch.
2. The DEK is used to encrypt specific fields in the event payload using **AES-GCM**.
3. The DEK itself is encrypted using an **AWS KMS Customer Master Key (CMK)** and stored alongside the encrypted data in the `eem` (Envelope Encryption Metadata) field.

## Envelope Encryption Metadata (eem)

The `eem` field in the `Event` interface stores the necessary information to decrypt the event:

```kotlin
@Serializable
data class EnvelopeEncryptionMetadata(
    val masterKeyAlias: String? = null,
    val dataKeys: Map<String, String>? = null, // region -> Base64 encrypted DEK
    val fields: List<String>? = null,           // list of encrypted fields
    val algorithm: String? = "AES/GCM/NoPadding"
)
```

### Multi-region Support

The `dataKeys` map allows storing the encrypted DEK for multiple AWS regions. This enables a service in one region to encrypt an event that can be decrypted by a service in another region, provided both regions have access to the same (or a replicated) KMS master key.

## Pipeline Integration

### Encrypting Events

You can encrypt specific fields of an event before publishing it by using the `encryptEvent` operator or by configuring the `CdcPipeline`.

#### Using CdcPipeline

```kotlin
CdcPipeline(
    // ...
    encryptionOptions = EncryptionOptions(
        eem = EnvelopeEncryptionMetadata(
            masterKeyAlias = "alias/my-key",
            fields = listOf("email", "phoneNumber")
        ),
        regions = listOf("us-east-1", "eu-west-1")
    )
)
```

#### Using the Operator

```kotlin
val encryptedFlow = flow.encryptEvent(
    options = EncryptionOptions(
        eem = EnvelopeEncryptionMetadata(
            masterKeyAlias = "alias/my-key",
            fields = listOf("sensitiveData")
        )
    ),
    fm = faultManager
)
```

### Decrypting Events

Incoming events can be automatically decrypted if they contain `eem` metadata.

#### Using CdcPipeline

Set `decrypt = true` in the `CdcPipeline` configuration to automatically decrypt incoming events.

```kotlin
CdcPipeline(
    // ...
    decrypt = true
)
```

#### Using the Operator

```kotlin
val decryptedFlow = flow.decryptEvent(fm = faultManager)
```

For DynamoDB Stream events (CDC), use `decryptChangeEvent`:

```kotlin
val decryptedFlow = flow.decryptChangeEvent(fm = faultManager)
```

## Manual Encryption/Decryption

If you need to encrypt or decrypt data manually (e.g., when saving a custom object to DynamoDB), you can use the `EventEncryption` utilities.

### Encrypting DynamoDB Maps

```kotlin
val item: Map<String, AttributeValue> = // ...
val options = EncryptionOptions(
    eem = EnvelopeEncryptionMetadata(
        masterKeyAlias = "alias/my-key",
        fields = listOf("secret")
    )
)

val encryptedItem = EventEncryption.encryptData(item, options)
// encryptedItem now contains an 'eem' field and the 'secret' field is encrypted
```

### Decrypting DynamoDB Maps

```kotlin
val decryptedItem = EventEncryption.decryptData(item)
```

## Fault Handling

If decryption fails (e.g., due to missing KMS permissions or corrupted data), the framework will catch the exception and create a `FaultEvent`. The original, undecrypted event is preserved in `UnitOfWork.undecryptedEvent` for diagnostic purposes.

## Security Considerations

- **KMS Permissions**: Ensure your Lambda execution role has `kms:GenerateDataKey`, `kms:Encrypt`, and `kms:Decrypt` permissions for the specified CMK.
- **Key Rotation**: AWS KMS handles CMK rotation. Since a new DEK is generated for each encryption operation, your data remains secure even if a DEK were to be compromised.
- **Algorithm**: The framework uses AES-GCM with a 128-bit authentication tag and a 12-byte random IV, providing both confidentiality and integrity.
