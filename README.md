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

### Expert Playbooks (Lead/Architect — full depth)

| Pattern | Expert playbook | Runnable example |
|---|---|---|
| Kafka | [`docs/kafka-expert-playbook.md`](docs/kafka-expert-playbook.md) | — |
| Strangler Fig | [`docs/strangler-fig-playbook.md`](docs/strangler-fig-playbook.md) | — |
| Metrics & Observability | [`docs/metrics-observability-playbook.md`](docs/metrics-observability-playbook.md) | — |
| Java Concurrency & Streams | [`docs/java-modern-concurrency-streams-playbook.md`](docs/java-modern-concurrency-streams-playbook.md) | — |
| Saga | [`docs/saga-expert-playbook.md`](docs/saga-expert-playbook.md) | `examples/saga/` |
| Transactional Outbox | [`docs/outbox-expert-playbook.md`](docs/outbox-expert-playbook.md) | `examples/outbox/OutboxDemo.java` |
| CQRS | [`docs/cqrs-expert-playbook.md`](docs/cqrs-expert-playbook.md) | — |
| LRU Cache | [`docs/lru-cache-expert-playbook.md`](docs/lru-cache-expert-playbook.md) | `examples/lru/` |
| Custom Spring Boot Starter | [`docs/custom-spring-boot-starter-expert-playbook.md`](docs/custom-spring-boot-starter-expert-playbook.md) | `examples/custom-spring-boot-starter/` |
| Circuit Breaker | [`docs/circuit-breaker-expert-playbook.md`](docs/circuit-breaker-expert-playbook.md) | `examples/circuit-breaker/CircuitBreakerDemo.java` |
| Bulkhead | [`docs/bulkhead-expert-playbook.md`](docs/bulkhead-expert-playbook.md) | `examples/bulkhead/BulkheadDemo.java` |

### Pattern summaries (quick reference)

1. Saga
- Short doc: `docs/saga.md`
- Expert playbook: `docs/saga-expert-playbook.md`
- Run: `javac examples/saga/SagaDemo.java && java -cp examples/saga SagaDemo`
- Advanced orchestrator doc (sync vs async): `docs/saga-orchestrator-sync-vs-async.md`
- Advanced run: `javac examples/saga/SagaOrchestratorModesDemo.java && java -cp examples/saga SagaOrchestratorModesDemo`
- Production-style reference examples (non-executable): `docs/saga-prod-examples-sync-async.md`

2. Transactional Outbox
- Short doc: `docs/outbox.md`
- Expert playbook: `docs/outbox-expert-playbook.md`
- Run: `javac examples/outbox/OutboxDemo.java && java -cp examples/outbox OutboxDemo`

3. Custom Spring Boot Starter
- Short doc: `docs/custom-spring-boot-starter.md`
- Expert playbook: `docs/custom-spring-boot-starter-expert-playbook.md`
- Covers: autoconfiguration, dependency management, transitive dependency pull-through, publishing, and production readiness
- Demo: `examples/custom-spring-boot-starter`
- Run: `cd examples/custom-spring-boot-starter && mvn clean verify && mvn -pl demo-consumer-app -am spring-boot:run`

4. LRU Cache (Capacity 5)
- Short doc: `docs/lru-cache.md`
- Expert playbook: `docs/lru-cache-expert-playbook.md`
- Run: `javac examples/lru/LruCacheDemo.java && java -cp examples/lru LruCacheDemo`
- Alternative implementation (LinkedHashMap): `javac examples/lru/LruCacheLinkedHashMapDemo.java && java -cp examples/lru LruCacheLinkedHashMapDemo`

5. CQRS
- Short doc: `docs/cqrs.md`
- Expert playbook: `docs/cqrs-expert-playbook.md`
- Covers: command model, read model, projectors, eventual consistency, rebuild strategy, and production operations

6. Circuit Breaker
- Short doc: `docs/circuit-breaker.md`
- Expert playbook: `docs/circuit-breaker-expert-playbook.md`
- Run: `javac examples/circuit-breaker/CircuitBreakerDemo.java && java -cp examples/circuit-breaker CircuitBreakerDemo`

7. Bulkhead
- Short doc: `docs/bulkhead.md`
- Expert playbook: `docs/bulkhead-expert-playbook.md`
- Run: `javac examples/bulkhead/BulkheadDemo.java && java -cp examples/bulkhead BulkheadDemo`

## Design Goal

Each pattern in this repository should answer five practical questions:

1. Why does the naive implementation fail in production?
2. What does the correct implementation look like?
3. What can still go wrong under retries, crashes, and duplicates?
4. Which metrics and logs are mandatory to operate it?
5. When should we avoid this pattern entirely?
