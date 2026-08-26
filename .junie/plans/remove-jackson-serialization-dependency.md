---
sessionId: session-260826-133746-83dk
---

# Requirements

### Overview & Goals
The goal is to completely remove Jackson as a serialization dependency from the project, replacing it with Kotlinx Serialization and reflection-based utilities where necessary.

### Scope
- **In Scope**:
    - Removal of Jackson dependencies from all Gradle modules.
    - Removal of `JacksonSerializationStrategy`, `JacksonEventCodec`, and related Jackson-specific code.
    - Refactoring of `SafeLogger` to use Kotlinx Serialization and `kotlin-reflect`.
    - Updating `CognitoAdapter` and `EventBridgeAdapter` to remove Jackson reliance.
    - Updating integration test facades in example projects.
- **Out of Scope**:
    - Removal of `java-uuid-generator` (unless it's later confirmed to be part of the request, but it's not used for JSON serialization).
    - Implementing a full-featured generic reflection-based JSON mapper (we only need enough for logging and basic PoJo conversion).

# Technical Design

### Current Implementation
Jackson is currently used as an optional serialization strategy. It is also used in `SafeLogger` to provide a "zero-fail" serialization for logging arbitrary objects, and in several adapters to handle PoJos from the AWS Lambda Java Events library.

### Proposed Changes
1.  **Serialization Strategy**: Kotlinx Serialization will become the primary (and currently only) serialization strategy. `SerializationStrategyResolver` will be simplified to remove Jackson detection.
2.  **SafeLogger & JsonUtils**: `SafeLogger` will be rewritten to use a recursive reflection-based approach (using `kotlin-reflect`) to convert arbitrary objects into a `JsonElement` tree, which is then serialized using Kotlinx `Json`. This allows us to keep the "zero-fail" logging while removing the Jackson dependency.
3.  **PoJo Handling**: For AWS Lambda events (which are standard Java PoJos), we will use reflection or existing snapshot DTOs to convert them to JSON-compatible structures.

### Key Decisions
- **Use `kotlin-reflect` for SafeLogger**: This avoids requiring all logged objects to be `@Serializable`, preserving the existing developer experience.
- **Remove Jackson-based AWS Record Serializers**: These were primarily used for Jackson interoperability, which is no longer required.

### File Structure Changes
- **Deleted**:
    - `core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonSerializationStrategy.kt`
    - `core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonEventCodec.kt`
    - `core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/aws/AwsRecordJackson.kt`
- **Modified**:
    - `core/build.gradle.kts`
    - `examples/sut/integration-test/build.gradle.kts`
    - `core/src/main/kotlin/io/github/huherto/awsLambdaStream/JsonUtils.kt`
    - `core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/SerializationStrategyResolver.kt`
    - `core/src/main/kotlin/io/github/huherto/awsLambdaStream/from/CognitoAdapter.kt`
    - `core/src/main/kotlin/io/github/huherto/awsLambdaStream/from/EventBridgeAdapter.kt`
    - `examples/sut/integration-test/src/integrationTest/kotlin/org/myorg/sut/facades/CheckHealthApiFacade.kt`

# Testing

### Validation Approach
- **Unit Tests**: All existing tests in the `core` module must pass. Tests that previously targeted Jackson serialization specifically will be removed or updated to target the generic strategy.
- **Integration Tests**: The integration tests in `examples/sut/integration-test` must pass, ensuring that the entire pipeline (including Cognito and EventBridge triggers) still works without Jackson.
- **SafeLogger Verification**: Verify that `SafeLogger` still correctly handles:
    - Circular references (if any, though not explicitly required, good to have).
    - Null values.
    - Complex PoJos like `AttributeValue` or `ScheduledEvent`.
    - Exceptions during reflection.

# Delivery Steps

### ✓ Step 1: Remove Jackson dependencies from build files
Remove all Jackson-related dependencies from build files.

- Update `core/build.gradle.kts` to remove `compileOnly(libs.jackson.kotlin)` and `testImplementation(libs.jackson.kotlin)`.
- Update `examples/sut/integration-test/build.gradle.kts` and other example modules to remove `implementation(libs.jackson.kotlin)`.
- Verify if any other module uses Jackson and remove it.
- Ensure `kotlinx-serialization-json` and `kotlin-reflect` are correctly scoped for the new requirements.

### ✓ Step 2: Remove Jackson-based classes and clean up core serialization logic
Delete Jackson implementation classes and clean up core serialization logic.

- Delete `core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonSerializationStrategy.kt`.
- Delete `core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonEventCodec.kt`.
- Delete `core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/aws/AwsRecordJackson.kt`.
- Remove Jackson-specific serializers/deserializers from `core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/RawRecordSerialization.kt`.
- Remove `SerializationStrategyKind.JACKSON` and related logic from `SerializationStrategyResolver.kt` and `SerializationConfig.kt`.

### ✓ Step 3: Refactor SafeLogger to remove Jackson dependency
Rewrite `SafeLogger` to use Kotlinx Serialization and reflection.

- Implement a reflection-based `toJsonElement` utility in `JsonUtils.kt` using `kotlin-reflect`.
- Update `SafeLogger.toJson` to use this utility, maintaining the "never throw" and "log anything" guarantee.
- Replace custom Jackson serializers (like `ByteBufferSerializer`, `AttributeValueSerializer`, `PipelineSerializer`) with equivalent logic in the new reflection-based approach.

### ✓ Step 4: Update Cognito and EventBridge adapters
Update adapters to use Kotlinx Serialization or reflection.

- Update `CognitoAdapter.kt` to replace `mapper.convertValue` with reflection-based Map conversion or direct Kotlinx Serialization.
- Update `EventBridgeAdapter.kt` to replace Jackson usage when processing `ScheduledEvent.detail`.
- Ensure all adapters correctly handle PoJo-to-JSON transitions without Jackson.

### ✓ Step 5: Update example project integration tests and validate
Update example project integration tests.

- Replace Jackson usage in `examples/sut/integration-test/src/integrationTest/kotlin/org/myorg/sut/facades/CheckHealthApiFacade.kt` with Kotlinx Serialization.
- Ensure all tests in the project still pass.