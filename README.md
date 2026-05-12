# Design Patterns: Deep Dive + Runnable Java Examples

This repository is a practical, interview-grade learning track for distributed systems and microservice design patterns.

## Learning Roadmap

1. Saga
2. Transactional Outbox
3. CQRS
4. Circuit Breaker
5. Bulkhead
6. Strangler Fig

## Repository Structure

- `docs/`: deep explanations, tradeoffs, edge cases, and production pitfalls
- `examples/`: runnable Java examples (happy path + failure path + compensation/recovery behavior)
- `templates/`: reusable building blocks for future patterns

## Current Patterns Implemented

1. Saga
- Doc: `docs/saga.md`
- Run: `javac examples/saga/SagaDemo.java && java -cp examples/saga SagaDemo`

2. Transactional Outbox
- Doc: `docs/outbox.md`
- Run: `javac examples/outbox/OutboxDemo.java && java -cp examples/outbox OutboxDemo`

## Design Goal

Each pattern in this repository should answer five practical questions:

1. Why does the naive implementation fail in production?
2. What does the correct implementation look like?
3. What can still go wrong under retries, crashes, and duplicates?
4. Which metrics and logs are mandatory to operate it?
5. When should we avoid this pattern entirely?
