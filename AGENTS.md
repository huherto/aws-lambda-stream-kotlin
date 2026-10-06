# AI Guidelines

## Project overview

This project provides Kotlin utilities and pipeline abstractions for AWS Lambda stream processing.

## Rules for AI assistants

- Prefer small, focused changes.
- Do not change public APIs unless explicitly requested.
- Preserve existing replay and fault-resubmission semantics.
- Add or update tests for behavior changes.
- Use Kotlin idioms and coroutines/Flow consistently.
- Avoid introducing new dependencies unless necessary.

## Unit tests
- Use arrange-act-assert style.
- Use kotest assertions.
- Use regular Junit tests annotations.
- test internal functions in isolation.

## Serialization
- Events need to be fully serializable/deserializable since they are used to communicate with other apps.
- UnitOfWork.record should be an instance of RawRecord
- UnitOfWork should be serializable with snapshots for diagnostic purposes.
- The framework uses kotlinx.serialization but consumers can choose to use other libraries.

## Implementing the Event interface
- If you need to implement Event subclasses, follow advice in docs/EventImplementationKotlin.md or docs/EventImplementationJava.md


## Running integration tests
```bash
cd examples/sut
docker compose up
# deploy all stacks
./deploy_stacks.sh -d

./run_itests.sh
# awslocal output is folder .awslocal_logs
ls .awslocal_logs
docker compose down
```


Use the Mac OS "say" command to inform e when tasks are complete or you need me to act.

