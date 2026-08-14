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
- test internal function in isolation.

## Serialization
- Events need to be fully serializable/deserializable since they are used to communicat with other apps.
- UnitOfWork.record should be an instance of 
- UnitOfWork should be serializable with snapshots for diagnostic purposes.

Use the Mac OS "say" command when tasks are complete or you need me to act.

