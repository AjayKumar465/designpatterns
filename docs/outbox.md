# Transactional Outbox Pattern

## Problem

A service updates its local database and must also publish an event.

Naive flow:

1. Write DB row
2. Publish event

Failure window:

- DB commit succeeds, event publish fails -> consumers never see event
- Event publish succeeds, DB commit fails -> consumers observe phantom state

## Core Idea

Write business data and outbound event into the same local transaction.
Then a separate publisher process reads unsent outbox rows and publishes them reliably.

## Flow

1. Business transaction writes:
- domain row(s)
- outbox row `{id, aggregateId, eventType, payload, status=PENDING}`

2. Outbox relay/poller:
- reads `PENDING` rows in order
- publishes to broker
- marks row as `SENT` (or stores sent timestamp)

3. Consumer side:
- must be idempotent (at-least-once delivery still causes duplicates)

## Reliability Rules

1. Outbox primary key is event id (global uniqueness)
2. Producer uses retries with backoff
3. Consumer keeps processed-event dedupe store
4. Relay is restart-safe
5. Monitoring for backlog growth

## Operational Metrics

1. `outbox_pending_count`
2. `outbox_publish_success_total`
3. `outbox_publish_failure_total`
4. `outbox_oldest_pending_age_seconds`
5. `consumer_duplicate_dropped_total`

## Tradeoffs

Pros:

1. No dual-write inconsistency
2. Strong producer reliability
3. Works with most brokers

Cons:

1. Extra table and relay process
2. Eventual consistency
3. Consumer idempotency still required

## Runnable Example

See `examples/outbox/OutboxDemo.java`.

It demonstrates:

1. Local transaction writes order + outbox event
2. Relay fails once and retries
3. Consumer de-duplicates duplicate event delivery
