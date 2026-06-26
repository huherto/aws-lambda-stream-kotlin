# Regional Health Check

## Purpose

The Regional Health Check provides a high-level health signal for a deployed regional service stack. It combines service-level health indicators from key AWS resources into a single regional status that can be used for monitoring, routing decisions, operational dashboards, and incident response.

The health check is designed to answer one primary question:

> Is this regional deployment healthy enough to receive and process traffic?

It does this by monitoring failures in critical resources such as API Gateway and DynamoDB, then aggregating those signals into a regional health result.

## High-Level Overview

The Regional Health Check is built around CloudWatch alarms and Route 53 health checks.

At a high level:

1. API Gateway emits service metrics to CloudWatch.
2. DynamoDB emits table metrics to CloudWatch.
3. CloudWatch alarms evaluate whether those resources are producing server-side errors.
4. Route 53 health checks track the state of those CloudWatch alarms.
5. A calculated Route 53 health check combines the child checks into one regional health status.
6. The regional health check ID and alarm are exposed as stack outputs for use by other systems.

## Main Resources

### API Gateway 5XX Alarm

API Gateway is monitored for `5XXError` responses.

A 5XX error indicates that the API layer is returning server-side failures. These failures may be caused by API Gateway itself, backend integrations, Lambda errors, authorization issues, or other service-side problems.

The alarm evaluates the sum of 5XX errors over a rolling window. If any 5XX errors occur during the configured evaluation period, the alarm enters the `ALARM` state.

This alarm is treated as a signal that the regional API path may be unhealthy.

### API Gateway Health Check

A Route 53 health check is attached to the API Gateway CloudWatch alarm.

This health check does not make a direct HTTP request to the API. Instead, it watches the CloudWatch alarm state. If the API Gateway 5XX alarm is healthy, the health check is healthy. If the alarm is in an alarm state, the health check becomes unhealthy.

This allows API health to be represented as a Route 53-compatible health signal.

### DynamoDB 5XX Alarm

DynamoDB is monitored for `SystemErrors`.

System errors represent service-side DynamoDB failures. These are different from expected client-side errors such as validation errors, conditional check failures, or throttling behavior.

The alarm evaluates whether DynamoDB reports system errors for the regional entities table. If system errors are detected during the configured evaluation period, the alarm enters the `ALARM` state.

This alarm is treated as a signal that the persistence layer may be unhealthy.

### DynamoDB Health Check

A Route 53 health check is attached to the DynamoDB CloudWatch alarm.

Like the API Gateway health check, this is a CloudWatch metric health check rather than an active request. It follows the state of the DynamoDB alarm and contributes to the overall regional health result.

### Regional Composite Alarm

A regional composite CloudWatch alarm combines the API Gateway and DynamoDB alarm states.

The regional alarm enters the `ALARM` state when either of the following is true:

- API Gateway is producing 5XX errors.
- DynamoDB is producing system errors.

This provides a single CloudWatch alarm that represents whether any critical regional dependency is showing server-side failures.

### Calculated Regional Health Check

The calculated Route 53 health check combines the child health checks into a single regional health check.

The child checks are:

- API Gateway health check
- DynamoDB health check

The calculated health check uses a health threshold that requires both child checks to be healthy. If either child check becomes unhealthy, the calculated regional health check becomes unhealthy.

This final calculated health check is the main regional health signal.

## Data Flow

The data flow is metric-driven rather than request-driven.
