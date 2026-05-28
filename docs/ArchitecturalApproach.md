
# Architectural Approach

Follow a structured approach to application architecture.

## Architectural Structure

A system is composed of one or more subsystems. Each subsystem is composed of one or more components.

Components communicate with each other through events. The event format defines the contract between components.

Similarly, subsystems communicate with each other through events or APIs. These communication mechanisms must have stable, well-defined contracts.

## Service Types

### Control Services

Control Services orchestrate the flow of events within a subsystem.

They coordinate business processes, determine how events should be handled, and trigger the appropriate downstream actions.

A Control Service includes three main components:

- A Listener Lambda function that receives and processes incoming events.
- A DynamoDB table that stores events for correlation and evaluation.
- A Trigger Lambda function that correlates stored events, evaluates, and emits higher-order events to initiate downstream actions.

![Control Service Diagram](images/ControlService.svg)


### Event Hubs

Event Hubs receive events from components within a subsystem and publish them to Kinesis Streams so they can be consumed by other components.

An Event Hub is typically composed of:

- AWS EventBridge
- One or more Kinesis Streams

Event Hubs provide a central point for event routing and distribution within a subsystem.

### External Service Gateways

External Service Gateways provide integration points between the system and external services.

They may receive events from other components, publish events to other components, or interact with external APIs.

External Service Gateways act as an Anticorruption Layer (ACL), translating between internal events, external events, and external APIs. This prevents external models or contracts from leaking into the internal architecture.

### Backends for Frontends (BFFs)

BFFs provide APIs to the outside world.  They expose client-specific interfaces and adapt internal system capabilities to the needs of external consumers such as web applications, mobile applications, or third-party clients.


