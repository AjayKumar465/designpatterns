# Design Patterns: Deep Dive + Runnable Java Examples

This repository is a practical, interview-grade learning track for distributed systems and microservice design patterns.

## Learning Roadmap

1. Saga
2. Transactional Outbox
3. Custom Spring Boot Starter
4. LRU Cache
5. CQRS
6. Circuit Breaker
7. Bulkhead
8. Strangler Fig

## Repository Structure

- `docs/`: deep explanations, tradeoffs, edge cases, and production pitfalls
- `examples/`: runnable Java examples (happy path + failure path + compensation/recovery behavior)
- `templates/`: reusable building blocks for future patterns

## Current Patterns Implemented

1. Saga
- Doc: `docs/saga.md`
- Run: `javac examples/saga/SagaDemo.java && java -cp examples/saga SagaDemo`
- Advanced orchestrator doc (sync vs async): `docs/saga-orchestrator-sync-vs-async.md`
- Advanced run: `javac examples/saga/SagaOrchestratorModesDemo.java && java -cp examples/saga SagaOrchestratorModesDemo`
- Production-style reference examples (non-executable): `docs/saga-prod-examples-sync-async.md`

2. Transactional Outbox
- Doc: `docs/outbox.md`
- Run: `javac examples/outbox/OutboxDemo.java && java -cp examples/outbox OutboxDemo`

3. Custom Spring Boot Starter
- Doc: `docs/custom-spring-boot-starter.md`
- Covers: autoconfiguration, dependency management, transitive dependency pull-through, publishing, and production readiness
- Demo: `examples/custom-spring-boot-starter`
- Run: `cd examples/custom-spring-boot-starter && mvn clean verify && mvn -pl demo-consumer-app -am spring-boot:run`

4. LRU Cache (Capacity 5)
- Doc: `docs/lru-cache.md`
- Run: `javac examples/lru/LruCacheDemo.java && java -cp examples/lru LruCacheDemo`
- Alternative implementation (LinkedHashMap): `javac examples/lru/LruCacheLinkedHashMapDemo.java && java -cp examples/lru LruCacheLinkedHashMapDemo`

## Design Goal

Each pattern in this repository should answer five practical questions:

1. Why does the naive implementation fail in production?
2. What does the correct implementation look like?
3. What can still go wrong under retries, crashes, and duplicates?
4. Which metrics and logs are mandatory to operate it?
5. When should we avoid this pattern entirely?
