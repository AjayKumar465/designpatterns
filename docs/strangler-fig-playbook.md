# Strangler Fig Pattern -- Expert Playbook (Lead/Architect Role, Java, 10+ Years Experience)

A comprehensive end-to-end reference covering the Strangler Fig pattern from theory to production-grade implementation. Includes real-world case studies (Netflix, Amazon, Shopify, GOV.UK), Java/Spring Boot code examples, data migration strategies, anti-corruption layers, testing patterns, and 40+ lead-level interview Q&As. Sourced from Martin Fowler's original writing, AWS/Azure architecture guides, Confluent, Medium, Reddit discussions, and production war stories.

---

## Table of Contents

1. [What Is the Strangler Fig Pattern?](#section-1-what-is-the-strangler-fig-pattern)
2. [Why Not a Big-Bang Rewrite?](#section-2-why-not-a-big-bang-rewrite)
3. [Core Components](#section-3-core-components)
4. [The Three Phases: Transform, Coexist, Eliminate](#section-4-the-three-phases-transform-coexist-eliminate)
5. [The Facade Layer -- Routing Strategies](#section-5-the-facade-layer----routing-strategies)
6. [Anti-Corruption Layer (ACL)](#section-6-anti-corruption-layer-acl)
7. [Data Migration Strategies](#section-7-data-migration-strategies)
8. [Feature Flags and Traffic Splitting](#section-8-feature-flags-and-traffic-splitting)
9. [Testing and Verification](#section-9-testing-and-verification)
10. [Event-Driven Strangler Fig (with Kafka)](#section-10-event-driven-strangler-fig-with-kafka)
11. [Spring Boot Implementation](#section-11-spring-boot-implementation)
12. [Spring Cloud Gateway Implementation](#section-12-spring-cloud-gateway-implementation)
13. [Kubernetes and Service Mesh (Istio)](#section-13-kubernetes-and-service-mesh-istio)
14. [Observability During Migration](#section-14-observability-during-migration)
15. [Production Pitfalls and How to Avoid Them](#section-15-production-pitfalls-and-how-to-avoid-them)
16. [Real-World Case Studies](#section-16-real-world-case-studies)
17. [Complementary Patterns](#section-17-complementary-patterns)
18. [Migration Runbook Template](#section-18-migration-runbook-template)
19. [Decision Framework: When to Use and When Not To](#section-19-decision-framework-when-to-use-and-when-not-to)
20. [Lead Interview Questions -- Logical and Production Scenarios](#section-20-lead-interview-questions----logical-and-production-scenarios)

---

## Section 1: What Is the Strangler Fig Pattern?

### 1.1 Origin

Coined by **Martin Fowler in 2004**, inspired by strangler fig trees in tropical forests. These trees grow around a host tree, sending roots down to the ground and branches up to the canopy, until the host tree dies and the fig stands on its own.

> "The alternative that my colleagues and I prefer, is to do a gradual process of modernization. Like the fig, it begins with small additions, often new features, that are built on top of, yet separate to the legacy code base." -- Martin Fowler

### 1.2 Definition

An architectural pattern for **incrementally replacing a legacy system** with a new system by:
1. Placing a **facade (proxy)** in front of the legacy system.
2. Building new functionality as **independent services** alongside the legacy system.
3. **Routing traffic** gradually from legacy to new services.
4. **Decommissioning** the legacy system when all traffic has been migrated.

### 1.3 The Pattern at a Glance

```
                     ┌─────────────┐
   Clients ──────►   │   Facade    │
                     │ (Gateway /  │
                     │   Proxy)    │
                     └──────┬──────┘
                            │
               ┌────────────┼────────────┐
               ▼                         ▼
      ┌─────────────┐          ┌─────────────┐
      │   Legacy     │          │    New       │
      │  Monolith    │          │  Services    │
      │  (shrinking) │          │  (growing)   │
      └──────┬───────┘          └──────┬───────┘
             │                         │
      ┌──────▼───────┐          ┌──────▼───────┐
      │   Legacy DB   │   CDC   │   New DB(s)   │
      │  (monolithic) │ ──────► │  (per-domain) │
      └──────────────┘          └───────────────┘
```

### 1.4 Key Principles

| Principle | Description |
|-----------|-------------|
| Incremental | Migrate one bounded context at a time |
| Reversible | Every migration step has a rollback path via the facade |
| Value-continuous | System stays in production throughout; no freeze periods |
| Client-transparent | External consumers never know about the backend migration |
| Data-first thinking | Data ownership migration is harder than code migration |

---

## Section 2: Why Not a Big-Bang Rewrite?

### 2.1 The Graveyard of Rewrites

| Company | What Happened | Outcome |
|---------|--------------|---------|
| Netscape (1998) | Scrapped Communicator 4.0, started fresh on Gecko | No major release for 3 years; Internet Explorer took the market |
| Segway (internal tools) | 2-year rewrite of internal manufacturing system | Delivered 18 months late, 3x over budget, missing critical features |
| Healthcare.gov (2013) | Big-bang launch of federal exchange | Catastrophic failure on day 1; required emergency rebuild |

### 2.2 Why Rewrites Fail

1. **Moving target**: Business requirements keep changing during the rewrite. By the time v2 ships, it's already outdated.
2. **Zero value until cutover**: No revenue from new system until the big-bang moment. Political pressure builds, scope creeps.
3. **Undocumented behavior**: Legacy systems contain years of implicit business rules. Reimplementing from specs misses edge cases.
4. **All-or-nothing risk**: If the cutover fails, you have no fallback (or a stale one).
5. **Team fatigue**: Multi-year rewrite projects drain morale and lose institutional knowledge as people leave.

### 2.3 Strangler Fig vs Big-Bang

| Dimension | Big-Bang Rewrite | Strangler Fig |
|-----------|-----------------|---------------|
| Risk profile | All-or-nothing | Incremental, reversible |
| Time to first value | Months/years | Weeks |
| System availability | Downtime during cutover | Zero downtime |
| Rollback capability | Limited/none | Per-feature rollback via facade |
| Team morale | Long grind, no visible progress | Regular wins, measurable progress |
| Business disruption | High (feature freeze during rewrite) | Low (new features go to new system) |
| Success rate | < 50% (industry-wide) | High (when discipline is maintained) |

---

## Section 3: Core Components

### 3.1 Component Overview

```
┌─────────────────────────────────────────────────────┐
│                    FACADE LAYER                      │
│  (API Gateway / Reverse Proxy / Spring Cloud Gateway)│
│  Routes traffic based on: path, headers, flags, %    │
└─────────────────────┬───────────────────────────────┘
                      │
        ┌─────────────┼──────────────┐
        ▼                            ▼
┌───────────────┐            ┌───────────────┐
│  LEGACY SYSTEM │            │  NEW SERVICES  │
│                │            │                │
│  ┌───────────┐ │            │  ┌───────────┐ │
│  │    ACL    │◄├────────────┤► │    ACL    │ │
│  │ (adapter) │ │            │  │ (adapter) │ │
│  └───────────┘ │            │  └───────────┘ │
│                │            │                │
│  ┌───────────┐ │    CDC     │  ┌───────────┐ │
│  │ Legacy DB │─┼───────────►│  │  New DB   │ │
│  └───────────┘ │            │  └───────────┘ │
└────────────────┘            └────────────────┘
```

### 3.2 Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| **Facade** | Single entry point; routes traffic to legacy or new system. Must be thin (no business logic). |
| **Legacy System** | The monolith being strangled. Continues to serve non-migrated domains. |
| **New Services** | Extracted microservices. Own their domain, data, and lifecycle. |
| **Anti-Corruption Layer (ACL)** | Translates between legacy and new interfaces. Prevents legacy domain model from polluting new services. |
| **Data Sync (CDC/Outbox)** | Keeps legacy and new databases consistent during transition period. |
| **Feature Flags** | Control which traffic goes where, per user/tenant/percentage. Enable instant rollback. |
| **Reconciliation Engine** | Background process comparing legacy and new system outputs. Detects drift before users do. |

---

## Section 4: The Three Phases: Transform, Coexist, Eliminate

### Phase 1: Transform

**Goal**: Build the new service alongside the legacy system.

1. **Identify the bounded context** to extract. Start with low-risk, high-value, loosely-coupled modules.
2. **Define the API contract** for the new service (OpenAPI/gRPC).
3. **Build the new service** independently. Do NOT copy the legacy data model -- design it properly for the domain.
4. **Deploy the facade** in front of the monolith (initially routes 100% to legacy).
5. **Set up data sync** (CDC or read from legacy DB via views).

### Phase 2: Coexist

**Goal**: Both systems run in parallel, serving traffic.

1. **Shadow testing**: Route copies of production traffic to the new service. Compare responses without affecting users.
2. **Canary release**: Route 1% -> 5% -> 25% -> 50% -> 100% of traffic to the new service.
3. **Monitor**: Compare latency (p50, p95, p99), error rates, and business KPIs between old and new.
4. **Reconciliation**: Background jobs compare data between legacy and new databases.
5. **Keep legacy as hot fallback**: Instant rollback by flipping the facade routing rule.

### Phase 3: Eliminate

**Goal**: Decommission the legacy functionality.

1. **Confirm 100% traffic** is on the new service for a defined stabilization period (e.g., 2-4 weeks).
2. **Remove the facade route** for this domain.
3. **Delete the legacy code** for the migrated feature. Do not leave dead code.
4. **Drop legacy database tables** for the migrated domain (after backup).
5. **Update documentation** and on-call runbooks.
6. **Celebrate the milestone** -- visible progress matters for team morale.

**Warning**: Phase 3 is where most teams fail. They extract the exciting parts but never finish the decommission. Five years later, both systems still run side by side with no plan to finish. Set a **cutover deadline with executive sponsorship** before you start.

---

## Section 5: The Facade Layer -- Routing Strategies

### 5.1 Routing Strategy Comparison

| Strategy | Mechanism | Complexity | Best For |
|----------|-----------|------------|----------|
| URL-path routing | `/v2/orders` goes to new service | Low | First extraction, clear API boundaries |
| Header-based routing | `X-Route: new-checkout` | Medium | Internal dogfooding, QA testing |
| Feature-flag routing | LaunchDarkly/Unleash per-user/tenant | High | Gradual rollout by user segment |
| Percentage rollout | 1% -> 5% -> 25% -> 100% | High | Near-zero-downtime cutover |
| User-bucket routing | Enterprise tenants first | Medium | B2B migrations with VIP customers |
| Content-based routing | Inspect request payload | High | Shared endpoints with mixed functionality |

### 5.2 NGINX Facade (Simplest)

```nginx
upstream legacy {
    server monolith.internal:8080;
}

upstream new_orders {
    server orders-service.internal:8080;
}

server {
    listen 80;

    # Migrated: orders go to new service
    location /api/orders {
        proxy_pass http://new_orders;
    }

    # Everything else still goes to monolith
    location / {
        proxy_pass http://legacy;
    }
}
```

### 5.3 Spring Cloud Gateway Facade

```java
@Configuration
public class StranglerFigGatewayConfig {

    @Bean
    public RouteLocator stranglerRoutes(RouteLocatorBuilder builder) {
        return builder.routes()
            // Migrated: orders service
            .route("orders-new", r -> r
                .path("/api/orders/**")
                .filters(f -> f
                    .circuitBreaker(cb -> cb
                        .setName("ordersBreaker")
                        .setFallbackUri("forward:/fallback/orders"))
                    .retry(retryConfig -> retryConfig
                        .setRetries(2)
                        .setBackoff(Duration.ofMillis(100),
                                    Duration.ofMillis(500), 2, true)))
                .uri("lb://ORDERS-SERVICE"))

            // Migrated: user service
            .route("users-new", r -> r
                .path("/api/users/**")
                .uri("lb://USER-SERVICE"))

            // Default: everything else goes to monolith
            .route("legacy-fallback", r -> r
                .path("/**")
                .uri("lb://LEGACY-MONOLITH"))
            .build();
    }

    // Fallback when new service is down -- route back to monolith
    @RestController
    public static class FallbackController {
        @RequestMapping("/fallback/orders")
        public ResponseEntity<String> ordersFallback() {
            // Route to monolith as fallback
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Orders service unavailable, routing to legacy");
        }
    }
}
```

### 5.4 Facade Design Rules

| Rule | Why |
|------|-----|
| Keep it thin | The facade routes traffic. It does NOT contain business logic. |
| No single point of failure | Use a managed API gateway (AWS API Gateway, GCP Cloud Endpoints) or deploy HA reverse proxy. |
| Declarative routing rules | Version routing rules alongside services in git. |
| Gateway replaceable | Do not build custom logic that ties you to one gateway product. |
| Auth and rate limiting at the facade | Cross-cutting concerns live here, not in individual services. |
| Every route has a rollback path | If the new service fails, the facade can instantly route back to legacy. |

---

## Section 6: Anti-Corruption Layer (ACL)

### 6.1 What Is It?

An adapter that translates between the legacy system's domain model and the new service's domain model. It prevents legacy concepts, naming conventions, and data structures from polluting the new clean design.

### 6.2 When You Need It

- Legacy monolith calls a migrated microservice but uses the old interface.
- New service needs data from the monolith but the monolith uses a different schema.
- Two domains have different ubiquitous languages (e.g., legacy calls it `customer_no`, new service calls it `userId`).

### 6.3 Java Implementation

```java
/**
 * ACL that translates between legacy User model and new User domain.
 * Lives inside the monolith to redirect calls to the migrated User microservice.
 */
@Component
public class UserAntiCorruptionLayer {

    private final UserMicroserviceClient newUserService;

    public UserAntiCorruptionLayer(UserMicroserviceClient newUserService) {
        this.newUserService = newUserService;
    }

    /**
     * Legacy code calls this method expecting the old interface.
     * ACL translates to the new service's API and maps the response back.
     */
    public LegacyUserDTO findUserByCustomerNumber(String customerNo) {
        // Translate legacy identifier to new domain
        NewUserResponse newUser = newUserService.getUserById(
            translateCustomerNoToUserId(customerNo)
        );

        // Map new domain model back to legacy DTO
        return LegacyUserDTO.builder()
            .customerNo(customerNo)
            .fullName(newUser.getFirstName() + " " + newUser.getLastName())
            .emailAddress(newUser.getEmail())
            .accountStatus(mapStatus(newUser.getStatus()))
            .build();
    }

    private String translateCustomerNoToUserId(String customerNo) {
        // Legacy uses "CUST-00001", new service uses UUID
        return mappingRepository.findUserIdByCustomerNo(customerNo)
            .orElseThrow(() -> new UserNotFoundException(
                "No mapping found for customer: " + customerNo));
    }

    private String mapStatus(UserStatus newStatus) {
        return switch (newStatus) {
            case ACTIVE -> "A";      // legacy uses single-char codes
            case SUSPENDED -> "S";
            case CLOSED -> "C";
        };
    }
}
```

### 6.4 ACL Placement

| Scenario | Where to Place ACL |
|----------|-------------------|
| Monolith calls migrated service | Inside the monolith (adapter wrapping new service client) |
| New service reads legacy data | Inside the new service (adapter wrapping legacy DB/API access) |
| Both directions | One ACL per direction -- never a shared translator |

### 6.5 ACL Best Practices

- The ACL is **temporary scaffolding**. Remove it when the legacy system is fully decommissioned.
- One ACL per bounded context boundary -- do not build a single giant translator.
- Test the ACL exhaustively -- it's where translation bugs hide.
- The ACL must handle **missing mappings gracefully** (not all legacy data may have new-system equivalents yet).

---

## Section 7: Data Migration Strategies

### 7.1 The Hardest Part

> "Data migration, not code, is the hard part." -- The HLD Handbook

Code extraction is straightforward compared to data migration. The monolithic database is shared across domains, with foreign keys, stored procedures, and triggers creating a web of dependencies.

### 7.2 Strategy Comparison

| Strategy | How It Works | Pros | Cons |
|----------|-------------|------|------|
| **Shared Database** | New service reads/writes the legacy DB directly | Simplest to start | No true independence. Schema coupling. Deploy together. |
| **Database Views** | New service reads via views on legacy DB | Read isolation, schema decoupling for reads | Still coupled for writes |
| **CDC (Change Data Capture)** | Stream committed changes from legacy DB WAL to new DB | Consistent, ordered, async, non-invasive | Infrastructure complexity (Debezium, Kafka) |
| **Dual Writes** | Application writes to both legacy and new DB | Seems simple | Partial failures cause data drift. No transactionality. Avoid. |
| **Transactional Outbox** | Write event + data in one DB transaction; relay publishes to new system | Atomic, reliable | Requires outbox table + CDC relay |
| **ETL + CDC** | Initial ETL for historical data, then CDC for ongoing sync | Complete data migration | Complex initial load coordination |

### 7.3 The Recommended Path (Per Domain)

```
Phase A: Shared Database
  New service reads from legacy DB via views.
  No write path yet.

Phase B: CDC Sync
  Set up CDC (Debezium) from legacy DB to new DB.
  New service writes to new DB.
  Legacy continues writing to legacy DB.
  Reconciliation job compares both DBs.

Phase C: Cutover
  New DB is the system of record.
  Legacy DB receives CDC from new DB (reverse sync, if needed).
  Monitor for data consistency.

Phase D: Cleanup
  Remove legacy tables for this domain.
  Remove CDC pipelines.
  New service is fully independent.
```

### 7.4 CDC with Debezium (Java / Kafka)

```java
// Debezium connector configuration for MySQL -> Kafka CDC
{
    "name": "legacy-user-cdc",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": "legacy-db.internal",
        "database.port": "3306",
        "database.user": "cdc_reader",
        "database.password": "${CDC_PASSWORD}",
        "database.server.id": "1",
        "topic.prefix": "legacy",
        "database.include.list": "monolith_db",
        "table.include.list": "monolith_db.users,monolith_db.user_addresses",
        "schema.history.internal.kafka.topic": "schema-changes.legacy",
        "schema.history.internal.kafka.bootstrap.servers": "kafka:9092"
    }
}
```

### 7.5 Reconciliation Job

```java
@Scheduled(fixedRate = 300_000) // every 5 minutes
public void reconcileUserData() {
    List<LegacyUser> legacyUsers = legacyRepo.findRecentlyModified(
        Instant.now().minus(10, ChronoUnit.MINUTES));

    int mismatches = 0;
    for (LegacyUser legacy : legacyUsers) {
        Optional<NewUser> newUser = newUserRepo.findByLegacyId(legacy.getId());

        if (newUser.isEmpty()) {
            log.warn("RECONCILIATION_MISS: Legacy user {} not found in new system",
                legacy.getId());
            mismatches++;
            continue;
        }

        if (!isConsistent(legacy, newUser.get())) {
            log.warn("RECONCILIATION_DRIFT: User {} differs. Legacy={}, New={}",
                legacy.getId(), legacy.getEmail(), newUser.get().getEmail());
            mismatches++;
            reconciliationMetrics.incrementDrift("users");
        }
    }

    if (mismatches > 0) {
        alertService.warn("User reconciliation found " + mismatches + " mismatches");
    }

    reconciliationMetrics.recordRun("users", legacyUsers.size(), mismatches);
}
```

### 7.6 Why NOT Dual Writes

| Problem | Description |
|---------|-------------|
| Partial failure | App writes to DB1 successfully, then DB2 write fails. Stores diverge. |
| No atomicity | There is no distributed transaction across two databases without 2PC (and nobody wants 2PC). |
| Ordering | Under load, write order to DB1 and DB2 can diverge. |
| Retry complexity | Retrying the failed write may succeed but with stale data if the entity was updated again. |

**Use CDC (Debezium) or the Outbox pattern instead.** The only authoritative write should go to one database. The other database is a derived replica until cutover.

---

## Section 8: Feature Flags and Traffic Splitting

### 8.1 Feature Flag Implementation

```java
@Component
public class MigrationFeatureFlags {

    private final UnleashClient unleash; // or LaunchDarkly, FF4J, ConfigCat

    /**
     * Determine if a request should be routed to the new service.
     * Supports per-user, per-tenant, and percentage-based rollout.
     */
    public boolean isNewServiceEnabled(String featureName, String userId) {
        UnleashContext context = UnleashContext.builder()
            .userId(userId)
            .addProperty("tenantId", resolveTenantId(userId))
            .build();

        return unleash.isEnabled(featureName, context);
    }

    /**
     * Gradual rollout: enable for specific tenant first,
     * then percentage-based rollout to all users.
     */
    public boolean isOrderServiceMigrated(String userId) {
        return isNewServiceEnabled("orders-service-v2", userId);
    }
}
```

### 8.2 Rollout Stages

| Stage | Traffic to New Service | Duration | Gate Criteria |
|-------|----------------------|----------|---------------|
| Shadow | 0% (shadow copy only) | 1-2 weeks | Response diff rate < 1% |
| Canary | 1-5% | 1 week | Error rate matches legacy, p95 latency within SLA |
| Ramp | 25% -> 50% | 2 weeks | Business KPIs unchanged (conversion rate, etc.) |
| Majority | 75% -> 95% | 1 week | Reconciliation shows 0 drift |
| Full | 100% | 2-4 weeks stabilization | No rollbacks needed |
| Decommission | 100% + remove legacy code | Sprint boundary | Team confidence, management sign-off |

### 8.3 Instant Rollback

The power of feature flags is **instant rollback without redeployment**:

```java
// If the new service starts failing, flip the flag
// All traffic immediately routes back to the monolith
// No code change, no deployment, no restart
unleash.setFeatureEnabled("orders-service-v2", false);
```

---

## Section 9: Testing and Verification

### 9.1 Shadow Traffic (Dark Launch)

The facade sends a copy of each request to the new service but only returns the legacy response to the client. The new service's response is logged for comparison.

```java
@Component
public class ShadowTrafficService {

    private final LegacyOrderService legacy;
    private final NewOrderServiceClient newService;
    private final ExecutorService shadowPool =
        Executors.newVirtualThreadPerTaskExecutor(); // Java 21+
    private final MeterRegistry metrics;

    public OrderResponse getOrder(String orderId) {
        // Primary: legacy response returned to client
        OrderResponse legacyResponse = legacy.getOrder(orderId);

        // Shadow: async call to new service, response compared but NOT returned
        shadowPool.submit(() -> {
            try {
                OrderResponse newResponse = newService.getOrder(orderId);
                compareResponses(orderId, legacyResponse, newResponse);
            } catch (Exception e) {
                metrics.counter("shadow.errors", "service", "orders").increment();
                log.warn("Shadow call failed for order {}", orderId, e);
            }
        });

        return legacyResponse;
    }

    private void compareResponses(String orderId,
                                   OrderResponse legacy,
                                   OrderResponse modern) {
        if (legacy.equals(modern)) {
            metrics.counter("shadow.match", "service", "orders").increment();
        } else {
            metrics.counter("shadow.mismatch", "service", "orders").increment();
            log.warn("SHADOW_MISMATCH order={}: legacy={}, new={}",
                orderId, legacy, modern);
        }
    }
}
```

### 9.2 Parallel Run (Comparison Testing)

Both systems process the same request. Responses are compared in real-time.

| Match Type | Description | Action |
|-----------|-------------|--------|
| Exact match | Responses are identical | Safe to proceed |
| Semantic match | Different format, equivalent data (e.g., date formatting) | Update comparison logic |
| Mismatch | Genuine discrepancy | Investigate before increasing traffic |

### 9.3 Verification Metrics Dashboard

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Shadow match rate | Comparison service | < 99% |
| New service error rate | New service APM | > legacy error rate |
| New service p95 latency | New service APM | > legacy p95 * 1.2 |
| Reconciliation drift count | Reconciliation job | > 0 sustained |
| Feature flag status | Flag management platform | Unexpected state change |
| Business KPI (conversion, revenue) | Analytics platform | > 2% deviation from baseline |

### 9.4 Contract Testing

Use consumer-driven contract tests to ensure the new service implements the same API contract as the legacy system:

```java
// Pact consumer test (in the facade/gateway)
@Pact(provider = "OrdersServiceV2", consumer = "ApiGateway")
public RequestResponsePact getOrderPact(PactDslWithProvider builder) {
    return builder
        .given("order 123 exists")
        .uponReceiving("a request for order 123")
        .path("/api/orders/123")
        .method("GET")
        .willRespondWith()
        .status(200)
        .body(new PactDslJsonBody()
            .stringType("orderId", "123")
            .stringType("status", "SHIPPED")
            .numberType("total", 99.99))
        .toPact();
}
```

---

## Section 10: Event-Driven Strangler Fig (with Kafka)

### 10.1 Why Event Streaming Fits

Kafka enables the legacy monolith and new microservices to **coexist through decoupled, event-driven communication**. Instead of the facade routing HTTP requests, events flow through Kafka topics, and both systems can consume them.

### 10.2 Architecture

```
┌───────────┐     events     ┌───────────────┐     events     ┌───────────┐
│  Legacy    │ ──────────────►│   Kafka       │ ──────────────►│   New     │
│  Monolith  │               │   Topics      │               │  Services  │
│            │◄──────────────│               │◄──────────────│            │
└───────────┘                └───────────────┘                └───────────┘
```

### 10.3 Phased Migration with Kafka

**Phase 1: Capture events from monolith**
- Add event publishing to the monolith (or use CDC with Debezium).
- New services consume events but do NOT serve live traffic yet.

**Phase 2: Shadow mode**
- New service processes events and stores results.
- Reconciliation compares new service results with legacy system.

**Phase 3: Gradual cutover**
- New service starts serving read traffic (facade routes reads to new service).
- Writes still go through monolith (events propagate to new service).

**Phase 4: Full ownership**
- New service owns both reads and writes.
- Monolith still publishes events for any remaining consumers.

**Phase 5: Decommission**
- Monolith stops publishing events for this domain.
- Legacy Kafka topics are deprecated and eventually deleted.

### 10.4 Outbox Pattern for Safe Event Publishing

```java
@Transactional
public Order createOrder(CreateOrderRequest request) {
    // 1. Business logic + DB write in one transaction
    Order order = orderRepository.save(mapToEntity(request));

    // 2. Write event to outbox table in the SAME transaction
    outboxRepository.save(OutboxEvent.builder()
        .aggregateId(order.getId().toString())
        .aggregateType("Order")
        .eventType("OrderCreated")
        .payload(objectMapper.writeValueAsString(order))
        .createdAt(Instant.now())
        .build());

    return order;
    // 3. Debezium CDC connector reads outbox table and publishes to Kafka
    // Guaranteed: if the order is saved, the event WILL be published
}
```

---

## Section 11: Spring Boot Implementation

### 11.1 Facade Controller Pattern

```java
@RestController
@RequestMapping("/api/accounts")
public class AccountFacadeController {

    private final LegacyAccountService legacy;
    private final NewAccountServiceClient newService;
    private final MigrationFeatureFlags flags;
    private final MeterRegistry metrics;

    public AccountFacadeController(LegacyAccountService legacy,
                                    NewAccountServiceClient newService,
                                    MigrationFeatureFlags flags,
                                    MeterRegistry metrics) {
        this.legacy = legacy;
        this.newService = newService;
        this.flags = flags;
        this.metrics = metrics;
    }

    @GetMapping("/{id}/balance")
    public ResponseEntity<BalanceResponse> getBalance(@PathVariable String id) {
        Timer.Sample sample = Timer.start(metrics);
        String route;

        try {
            if (flags.isNewServiceEnabled("account-service-v2", id)) {
                route = "new";
                BalanceResponse response = newService.getBalance(id);
                return ResponseEntity.ok(response);
            } else {
                route = "legacy";
                BalanceResponse response = legacy.getBalance(id);
                return ResponseEntity.ok(response);
            }
        } finally {
            sample.stop(Timer.builder("account.balance.request")
                .tag("route", route)
                .register(metrics));
        }
    }

    @PostMapping("/{id}/deposit")
    public ResponseEntity<Void> deposit(@PathVariable String id,
                                         @RequestBody DepositRequest request) {
        if (flags.isNewServiceEnabled("account-service-v2", id)) {
            newService.deposit(id, request);
        } else {
            legacy.deposit(id, request.amount());
        }
        return ResponseEntity.ok().build();
    }
}
```

### 11.2 New Service Client with Circuit Breaker

```java
@Component
public class NewAccountServiceClient {

    private final WebClient webClient;

    public NewAccountServiceClient(
            @Value("${services.account.url}") String baseUrl) {
        this.webClient = WebClient.builder()
            .baseUrl(baseUrl)
            .build();
    }

    @CircuitBreaker(name = "accountService", fallbackMethod = "getBalanceFallback")
    @Retry(name = "accountService")
    public BalanceResponse getBalance(String accountId) {
        return webClient.get()
            .uri("/api/v2/accounts/{id}/balance", accountId)
            .retrieve()
            .bodyToMono(BalanceResponse.class)
            .block(Duration.ofSeconds(5));
    }

    public BalanceResponse getBalanceFallback(String accountId, Throwable t) {
        log.warn("New account service failed for {}, falling back to legacy", accountId, t);
        throw new FallbackToLegacyException(accountId);
    }
}
```

### 11.3 Handling Fallback to Legacy

```java
@ExceptionHandler(FallbackToLegacyException.class)
public ResponseEntity<BalanceResponse> handleFallback(
        FallbackToLegacyException ex) {
    BalanceResponse response = legacy.getBalance(ex.getAccountId());
    return ResponseEntity.ok()
        .header("X-Served-By", "legacy-fallback")
        .body(response);
}
```

---

## Section 12: Spring Cloud Gateway Implementation

### 12.1 Weight-Based Traffic Splitting

```yaml
spring:
  cloud:
    gateway:
      routes:
        # Canary: 10% to new service, 90% to legacy
        - id: orders-canary-new
          uri: lb://ORDERS-SERVICE
          predicates:
            - Path=/api/orders/**
            - Weight=orders-group, 10
          filters:
            - AddResponseHeader=X-Served-By, new-service

        - id: orders-canary-legacy
          uri: lb://LEGACY-MONOLITH
          predicates:
            - Path=/api/orders/**
            - Weight=orders-group, 90
          filters:
            - AddResponseHeader=X-Served-By, legacy
```

### 12.2 Header-Based Routing (For QA/Dogfooding)

```yaml
spring:
  cloud:
    gateway:
      routes:
        # Internal testing: header forces new service
        - id: orders-dogfood
          uri: lb://ORDERS-SERVICE
          predicates:
            - Path=/api/orders/**
            - Header=X-Route-To, new-service
          order: 0  # higher priority

        # Default: legacy
        - id: orders-default
          uri: lb://LEGACY-MONOLITH
          predicates:
            - Path=/api/orders/**
          order: 1
```

---

## Section 13: Kubernetes and Service Mesh (Istio)

### 13.1 Istio VirtualService for Traffic Splitting

```yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: orders-routing
spec:
  hosts:
    - orders.example.com
  http:
    - match:
        - uri:
            prefix: /api/orders
      route:
        - destination:
            host: orders-service-new
            port:
              number: 8080
          weight: 10
        - destination:
            host: legacy-monolith
            port:
              number: 8080
          weight: 90
```

### 13.2 Gradual Cutover Process

```bash
# Phase 1: 100% legacy (baseline)
kubectl apply -f virtualservice-orders-100-legacy.yaml

# Phase 2: Shadow traffic (mirror, not split)
kubectl apply -f virtualservice-orders-mirror.yaml

# Phase 3: Canary (10/90)
kubectl apply -f virtualservice-orders-10-90.yaml

# Phase 4: Ramp (50/50)
kubectl apply -f virtualservice-orders-50-50.yaml

# Phase 5: Full (100% new)
kubectl apply -f virtualservice-orders-100-new.yaml

# Phase 6: Remove legacy route
kubectl apply -f virtualservice-orders-new-only.yaml
```

### 13.3 Istio Traffic Mirroring (Shadow Mode)

```yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: orders-shadow
spec:
  hosts:
    - orders.example.com
  http:
    - route:
        - destination:
            host: legacy-monolith
      mirror:
        host: orders-service-new
      mirrorPercentage:
        value: 100.0
```

All traffic goes to legacy, but a copy is mirrored to the new service. The new service's response is discarded. Perfect for validating behavior without risk.

---

## Section 14: Observability During Migration

### 14.1 Required Dashboards

| Dashboard | Panels | Purpose |
|-----------|--------|---------|
| Migration Progress | % traffic on new vs legacy per domain | Executive visibility |
| Comparative Latency | p50/p95/p99 for legacy and new side-by-side | Performance parity |
| Error Rate Comparison | Error rate for legacy and new side-by-side | Correctness parity |
| Reconciliation Health | Drift count, last run time, mismatch trends | Data consistency |
| Business KPIs | Conversion rate, order volume, revenue | Business impact |

### 14.2 Key Metrics

```java
// Tag every request with the routing decision
metrics.counter("request.routed",
    "service", serviceName,
    "route", isNewService ? "new" : "legacy",
    "domain", "orders"
).increment();

// Track comparison results
metrics.counter("shadow.result",
    "outcome", matchResult, // "match", "semantic_match", "mismatch"
    "domain", "orders"
).increment();

// Track migration progress
metrics.gauge("migration.traffic_percent_new",
    Tags.of("domain", "orders"),
    percentOnNew);
```

### 14.3 Distributed Tracing

Add a custom span attribute to indicate which system handled the request:

```java
Span.current().setAttribute("migration.route", isNewService ? "new" : "legacy");
Span.current().setAttribute("migration.domain", "orders");
```

This lets you filter traces by routing path in Jaeger/Zipkin and compare performance characteristics.

---

## Section 15: Production Pitfalls and How to Avoid Them

### Pitfall 1: Perpetual Dual Implementation

**Symptom**: Two teams fix two copies of the same business rule. They eventually diverge in production.

**Fix**: Set a **cutover deadline with executive sponsorship** before starting. Track % of traffic on the new system weekly. If it plateaus below 100% for more than a quarter, escalate. The strangler only pays off when you **finish the strangle**.

### Pitfall 2: Strangling Code But Not Data

**Symptom**: New services call the legacy database directly. You have independent deployments but shared data. Schema changes in one break the other.

**Fix**: Include data migration in each slice's definition of done. Use CDC or the outbox pattern to establish data independence. See Section 7.

### Pitfall 3: No Parity Tests

**Symptom**: Subtle behavioral differences between old and new systems surface as revenue loss, silent data corruption, or customer complaints.

**Fix**: Shadow traffic + automated comparison + reconciliation. Do NOT increase traffic beyond canary until shadow match rate exceeds 99%. See Section 9.

### Pitfall 4: Facade Becomes a New Monolith

**Symptom**: Business logic creeps into the API gateway. The facade becomes the bottleneck and the hardest component to change.

**Fix**: The facade routes traffic. Period. No transformations, no business rules, no aggregation. Use ACLs inside services for translation.

### Pitfall 5: No Deletion Ceremony

**Symptom**: Legacy code and database tables linger long after migration. On-call still wakes for dead code paths.

**Fix**: Make legacy code deletion a tracked task in each migration sprint. Remove dead routes, dead code, dead tables. Update runbooks. Celebrate deletions.

### Pitfall 6: Gateway Deployed, Nothing Migrated

**Symptom**: You shipped infrastructure (API gateway, service mesh, monitoring) but no actual functionality has moved.

**Fix**: The first extraction should happen within 2-4 weeks of deploying the facade. If you spend 3 months on infrastructure before migrating a single endpoint, you are over-engineering.

### Pitfall 7: New Service Copies Legacy Design

**Symptom**: The new service replicates the legacy database schema and code patterns. You rebuild the monolith as microservices.

**Fix**: The strangler fig is an opportunity to redesign. Use proper domain-driven design for the new bounded contexts. The ACL exists specifically to translate between old and new designs.

### Pitfall 8: Ignoring Cross-Cutting Concerns

**Symptom**: Authentication, authorization, rate limiting, and logging work differently in legacy and new services. Users get inconsistent experiences.

**Fix**: Implement cross-cutting concerns at the facade layer. Auth tokens should be validated once at the gateway. Logging correlation IDs should propagate through both paths.

---

## Section 16: Real-World Case Studies

### 16.1 Netflix (2008-2016)

| Aspect | Details |
|--------|---------|
| Starting point | Single monolithic Java WAR (`monolith.war`) |
| Duration | 8 years |
| End state | 700+ microservices |
| Approach | New features as microservices first, then gradual extraction |
| Key tools | Zuul (API gateway), Eureka (service discovery), Hystrix (circuit breaker) |
| Last migrated | User management service (2016) |
| Key lesson | Built Chaos Engineering (Chaos Monkey) to ensure resilience during transition |

### 16.2 Amazon

| Aspect | Details |
|--------|---------|
| Starting point | Monolithic bookstore application |
| Approach | Each team extracted their domain into an independent service |
| Key insight | Two-pizza team rule -- each service owned by a team small enough to be fed by two pizzas |
| Result | SOA/microservices architecture, eventually led to AWS services being products |

### 16.3 GOV.UK (2013-2014)

| Aspect | Details |
|--------|---------|
| Starting point | 312 government agency websites, 685 domains |
| Duration | 15 months |
| Facade | "Bouncer" -- a Ruby/Rack application |
| Data store | PostgreSQL database of 1.8+ million URL mappings |
| CDN | Fastly absorbed 70% of traffic |
| Admin UI | "Transition" -- a Rails app for transition managers to edit URL mappings |
| Result | All agencies migrated to a single GOV.UK platform |
| Key lesson | Facade + URL redirect mapping can handle even nation-scale migrations |

### 16.4 Shopify

| Aspect | Details |
|--------|---------|
| Starting point | Large Ruby on Rails monolith (1M+ merchants) |
| Approach | Internal "components" pattern -- modular boundaries within the monolith first |
| Extraction order | Storefront rendering was one of the last pieces (highest risk) |
| Key lesson | Create seams (module boundaries) inside the monolith before extracting |
| Pattern used | Strangler Fig applied at the code level (model refactoring) and the service level |

---

## Section 17: Complementary Patterns

### 17.1 Patterns That Work Together

| Pattern | How It Complements Strangler Fig |
|---------|--------------------------------|
| **Anti-Corruption Layer** | Translates between legacy and new domain models. Prevents legacy pollution. |
| **Circuit Breaker** | Protects the facade when the new service is unavailable. Enables automatic fallback to legacy. |
| **Feature Flags** | Controls traffic routing per user/tenant/percentage. Enables instant rollback. |
| **Transactional Outbox** | Guarantees reliable event publishing from the monolith during migration. |
| **CDC (Change Data Capture)** | Syncs data from legacy DB to new DB without dual writes. |
| **CQRS** | Separates read and write paths -- often the first seam to exploit in a monolith. |
| **Saga** | Manages distributed transactions across legacy and new services during transition. |
| **Backend for Frontend (BFF)** | BFF can serve as the strangler facade for frontend-specific APIs. |
| **Branch by Abstraction** | Introduce an abstraction in the monolith, then swap the implementation from legacy to new. |

### 17.2 Branch by Abstraction (Inside the Monolith)

When you can't deploy a separate service yet, use Branch by Abstraction within the monolith:

```java
// Step 1: Extract interface
public interface PaymentProcessor {
    PaymentResult process(PaymentRequest request);
}

// Step 2: Legacy implementation
@Component
@ConditionalOnProperty(name = "payment.version", havingValue = "legacy")
public class LegacyPaymentProcessor implements PaymentProcessor {
    public PaymentResult process(PaymentRequest request) { /* old code */ }
}

// Step 3: New implementation
@Component
@ConditionalOnProperty(name = "payment.version", havingValue = "v2")
public class NewPaymentProcessor implements PaymentProcessor {
    public PaymentResult process(PaymentRequest request) { /* new code */ }
}

// Step 4: Switch via config -- no code change needed
// application.yml: payment.version=v2
```

---

## Section 18: Migration Runbook Template

### Per-Domain Migration Checklist

| Phase | Task | Owner | Done |
|-------|------|-------|------|
| **Planning** | Identify bounded context and data dependencies | Architect | |
| | Define API contract (OpenAPI spec) | Backend lead | |
| | Define exit criteria (latency, error rate, business KPIs) | Product + Eng | |
| | Set cutover deadline with stakeholders | Engineering Manager | |
| **Build** | Implement new service | Dev team | |
| | Implement ACL (if needed) | Dev team | |
| | Set up CDC pipeline (Debezium) | Platform team | |
| | Write reconciliation job | Dev team | |
| | Write contract tests (Pact) | Dev team | |
| **Verify** | Deploy shadow traffic | DevOps | |
| | Monitor shadow match rate for 1-2 weeks | On-call | |
| | Fix all mismatches | Dev team | |
| | Shadow match rate >= 99% | Gate | |
| **Rollout** | Canary: 5% traffic to new service | DevOps | |
| | Monitor error rates, latency, business KPIs for 1 week | On-call | |
| | Ramp: 25% -> 50% -> 75% -> 100% | DevOps | |
| | Each ramp gate: metrics within SLA for 48 hours | Gate | |
| **Stabilize** | 100% traffic on new service for 2-4 weeks | On-call | |
| | Reconciliation shows 0 drift for 2 weeks | Gate | |
| **Decommission** | Remove legacy route from facade | DevOps | |
| | Delete legacy code for this domain | Dev team | |
| | Drop legacy database tables (after backup) | DBA | |
| | Remove CDC pipeline | Platform team | |
| | Update runbooks and documentation | Dev team | |
| | Retrospective | All | |

### Timeline Estimates

| Migration Size | Typical Duration |
|---------------|-----------------|
| Single endpoint/feature | 2-4 weeks |
| Small bounded context (3-5 endpoints) | 1-3 months |
| Medium domain (10-20 endpoints + DB) | 3-6 months |
| Full monolith decomposition | 12-36 months |
| Enterprise-scale (Netflix, GOV.UK) | 1-8 years |

---

## Section 19: Decision Framework: When to Use and When Not To

### When to Use

- Migrating a monolith to microservices incrementally.
- System must remain available throughout migration (no downtime windows).
- Business cannot tolerate a feature freeze during rewrite.
- Team needs to deliver new features while migrating.
- Risk tolerance is low -- need per-feature rollback capability.
- Multiple teams need to work on different domains independently.

### When NOT to Use

| Scenario | Why Not | Alternative |
|----------|---------|-------------|
| Small, simple monolith | Overhead of facade + dual systems not justified | Direct rewrite or refactor in place |
| No clear domain boundaries | Cannot identify what to extract | Invest in domain analysis first (Event Storming) |
| Monolith has no API layer | Cannot intercept requests without significant refactoring | Build API layer first (Branch by Abstraction) |
| Team lacks operational maturity | Running two systems is harder than running one | Improve DevOps capabilities first |
| Budget for < 6 months | Strangler Fig requires sustained investment | Not enough time to complete the strangle |
| Legacy system is being sunset entirely | No value in incremental migration | Direct cutover to replacement |

---

## Section 20: Lead Interview Questions -- Logical and Production Scenarios

### Category A: Fundamentals

**Q1: What is the Strangler Fig pattern and why is it named that way?**

A: Named by Martin Fowler (2004) after strangler fig trees that grow around a host tree and eventually replace it. In software, a new system is built alongside the legacy system, with a facade routing traffic between them. Traffic shifts gradually from legacy to new until the legacy system can be decommissioned. It's the opposite of a big-bang rewrite -- incremental, reversible, and value-continuous. See [Section 1](#section-1-what-is-the-strangler-fig-pattern).

**Q2: What are the three phases of the Strangler Fig pattern?**

A: **Transform** (build the new service alongside legacy), **Coexist** (run both in parallel with gradual traffic shift), **Eliminate** (decommission legacy code and data). The critical insight is that Phase 3 (Eliminate) is where most teams fail -- they extract the exciting parts but never finish the decommission. See [Section 4](#section-4-the-three-phases-transform-coexist-eliminate).

**Q3: What is the role of the facade in the Strangler Fig pattern?**

A: The facade is the single entry point for all client traffic. It decides per-request whether to route to legacy or new system. Clients never know about the backend migration. It can be an NGINX reverse proxy, an API gateway (Spring Cloud Gateway, AWS API Gateway), or a service mesh (Istio VirtualService). The facade must be thin -- no business logic. See [Section 5](#section-5-the-facade-layer----routing-strategies).

---

### Category B: Architecture and Design

**Q4: How do you decide which part of the monolith to extract first?**

A: Use a prioritization matrix considering four factors:
1. **Business value**: Which domain benefits most from modernization (scalability, new features)?
2. **Coupling**: Which module has the fewest dependencies on other modules? Start with loosely coupled.
3. **Risk**: Start with low-risk, non-critical domains to learn the migration process.
4. **Technical debt**: Modules with highest maintenance cost benefit most from extraction.
The ideal first candidate is: high business value, low coupling, low risk, high tech debt. See Sections [4](#section-4-the-three-phases-transform-coexist-eliminate) and [16](#section-16-real-world-case-studies).

**Q5: How do you handle the database during a Strangler Fig migration?**

A: The database is the hardest part. Follow this path per domain:
1. Start with the new service reading from the legacy DB via views (shared database).
2. Set up CDC (Debezium) to stream changes from legacy DB to a new domain database.
3. New service writes to its own database; reconciliation job validates consistency.
4. Cut over: new DB becomes the system of record.
5. Remove legacy tables for this domain.
Never use dual writes -- they cause data drift without transactional guarantees. See [Section 7](#section-7-data-migration-strategies).

**Q6: What is an Anti-Corruption Layer and when do you need it?**

A: An ACL is an adapter that translates between legacy and new domain models. You need it when the monolith calls a migrated microservice using the old interface, or when the new service needs to read legacy data with a different schema. It prevents legacy concepts from polluting the new clean design. The ACL is temporary scaffolding -- remove it when legacy is decommissioned. See [Section 6](#section-6-anti-corruption-layer-acl).

**Q7: How does the Strangler Fig pattern differ from the Branch by Abstraction pattern?**

A: Strangler Fig deploys a **separate service** behind a routing facade. Branch by Abstraction creates an **abstraction within the monolith** and swaps the implementation. Use Branch by Abstraction when you can't deploy a separate service yet (organizational constraints, shared state). Use Strangler Fig when you want independent deployment and scaling. They can be combined: start with Branch by Abstraction to create seams, then extract the new implementation as a separate service. See [Section 17.2](#172-branch-by-abstraction-inside-the-monolith).

---

### Category C: Production Scenarios

**Q8: You've migrated the orders service. After ramping to 50% traffic, you notice the new service returns different totals than the legacy system for 0.3% of orders. What do you do?**

A: Do NOT roll forward to 100%. Steps:
1. Immediately halt the ramp at 50% (or reduce to canary 5%).
2. Analyze the 0.3% mismatches: are they rounding differences, timezone issues, tax calculation bugs, or genuine logic errors?
3. If rounding/formatting: update the comparison logic (semantic match).
4. If logic error: fix the new service, deploy, re-validate with shadow traffic.
5. Only resume ramp when shadow match rate returns to 99%+.
6. Add the specific edge case to contract tests to prevent regression.
See [Section 9](#section-9-testing-and-verification).

**Q9: The new orders service goes down during a traffic ramp (50/50 split). What happens?**

A: If the facade has circuit breaker + fallback configured:
1. Circuit breaker trips after detecting failures.
2. Facade automatically routes 100% traffic back to legacy.
3. Users experience zero impact (at most, slightly higher latency during the switch).
4. Alert fires. On-call investigates the new service failure.
5. After fixing and deploying, reset the circuit breaker and resume the ramp from canary (5%).
If NO circuit breaker is configured: 50% of users get errors. This is why circuit breakers in the facade are non-negotiable. See [Section 11.2](#112-new-service-client-with-circuit-breaker).

**Q10: Your team extracted 3 services over 6 months, but the remaining 80% of the monolith hasn't been touched. Management wants to know when it will be done.**

A: This is the **stalled migration** anti-pattern. Responses:
1. Track and report % of traffic on new services vs legacy weekly. Visualize the trend.
2. Identify why extraction slowed: is it data coupling? Lack of domain knowledge? Organizational resistance?
3. Propose a revised roadmap with quarterly milestones, each extracting 1-2 domains.
4. Get executive sponsorship for a decommission date. Without a deadline, migrations stall forever.
5. Consider whether the remaining monolith is "good enough" -- not everything needs to be microservices. The pragmatic answer might be to stabilize the monolith for low-value domains.
See [Section 15 Pitfall 1](#pitfall-1-perpetual-dual-implementation).

**Q11: During migration, the legacy monolith needs to call a feature you already migrated to a microservice. How do you handle this?**

A: Use an Anti-Corruption Layer inside the monolith:
1. The ACL acts as an adapter, implementing the old internal interface.
2. Internally, it makes an HTTP/gRPC call to the new microservice.
3. It translates the response back to the legacy data model.
4. The rest of the monolith is unaware of the migration -- no changes needed to calling code.
This is exactly what AWS describes as the ACL pattern within Strangler Fig. See [Section 6](#section-6-anti-corruption-layer-acl).

---

### Category D: Data and Consistency

**Q12: How do you prevent data inconsistency during the coexist phase?**

A: Three-layer approach:
1. **Single writer principle**: Only one system writes to a given domain at any time. Use CDC to sync to the other system's read replica.
2. **Reconciliation jobs**: Scheduled background process that compares data in both systems and alerts on drift.
3. **Idempotent operations**: Both legacy and new systems must handle replayed events/requests without side effects.
Never use dual writes (both systems writing simultaneously) -- partial failures cause silent data divergence. See [Section 7.6](#76-why-not-dual-writes).

**Q13: How do you handle foreign key relationships that span the monolith and the new service?**

A: During migration, cross-boundary relationships are inevitable. Strategies:
1. **Denormalize at the boundary**: The new service stores a copy of the foreign entity's ID and essential data. Keep it synced via CDC.
2. **API call at read time**: The new service calls the monolith's API (via ACL) to resolve the foreign entity. Slower but always consistent.
3. **Event-carried state transfer**: The monolith publishes events when the related entity changes. The new service maintains a local read replica.
4. **Accept eventual consistency**: For non-critical relationships, a brief delay in sync is acceptable.

**Q14: You're migrating the payments domain. The monolith's payments table has 500 million rows. How do you migrate the data?**

A: Two-phase approach:
1. **Initial bulk load (ETL)**: During a low-traffic window, run an ETL job to copy historical data from the legacy payments table to the new service's database. Transform the schema as needed.
2. **Ongoing sync (CDC)**: Set up Debezium CDC to stream all changes from the legacy payments table to the new database, starting from the point where the ETL ended. This catches any changes that happened during the bulk load.
3. **Reconciliation**: Run a checksums-based comparison between both databases to verify the migration was complete and correct.
4. **Cutover**: Once reconciliation passes, switch the new service to be the system of record for payments.
Timeline estimate for 500M rows: 2-4 weeks for ETL + validation. See [Section 7.3](#73-the-recommended-path-per-domain).

---

### Category E: Tooling and Implementation

**Q15: What technology would you use for the facade layer?**

A: Depends on the environment:
- **NGINX/Envoy**: Simplest for path-based routing. Low overhead. No business logic.
- **Spring Cloud Gateway**: Best for Java/Spring ecosystems. Supports programmatic routing, circuit breakers, rate limiting.
- **Istio VirtualService**: Best for Kubernetes environments. Traffic splitting, mirroring, canary built-in.
- **AWS API Gateway + Refactor Spaces**: Managed, serverless. Best for AWS-native migrations.
- **Kong/Apigee**: Enterprise API management with analytics and developer portal.
The key rule: keep the facade infrastructure-focused. No business logic. See [Section 5](#section-5-the-facade-layer----routing-strategies).

**Q16: How do you implement a rollback if the new service fails?**

A: Three layers of rollback:
1. **Feature flag flip**: Disable the flag instantly. All traffic goes to legacy. Zero deployment needed.
2. **Gateway routing update**: Change the weight from 50/50 to 0/100 (all legacy). Applies in seconds.
3. **Circuit breaker auto-fallback**: If the new service errors out, the circuit breaker trips automatically and routes to legacy.
All three should be in place before any production traffic hits the new service. See [Sections 8.3](#83-instant-rollback) and [11.2](#112-new-service-client-with-circuit-breaker).

---

### Category F: Organizational and Process

**Q17: How do you organize teams during a Strangler Fig migration?**

A: Two models:
1. **Dedicated migration team**: A small team (3-5 engineers) owns the migration process. They extract services, set up infrastructure, and hand off to domain teams once stable. Risk: migration team becomes a bottleneck.
2. **Domain teams own their extraction**: Each domain team extracts their own bounded context into a new service. The platform team provides the facade, CDC, and tooling. Risk: inconsistent approaches across teams.
Best practice: platform team provides the facade, CDC, and migration playbook. Domain teams execute the extraction for their bounded context using the playbook. Weekly sync to share learnings.

**Q18: How do you get buy-in for a Strangler Fig migration?**

A: Speak the language of business value:
1. Show the cost of the current system: deployment frequency, incident count, time-to-market for new features.
2. Propose the first extraction as a **time-boxed experiment** (4-6 weeks). Tangible ROI: reduced deployment time, faster feature delivery.
3. Track and report migration metrics weekly: % traffic migrated, deployment frequency improvement, incident reduction.
4. Make the business case for each subsequent extraction based on the ROI of the previous one.
5. Get an executive sponsor who commits to the decommission timeline.

**Q19: How do you handle feature development during the migration? Do you freeze the monolith?**

A: NO feature freeze. This is one of the key advantages of Strangler Fig:
1. **New features**: Build in the new service if the domain is already migrated. Build in the monolith if the domain is not yet migrated.
2. **Bug fixes**: Fix in whichever system currently serves traffic for that domain.
3. **Rule**: Never build the same feature in both systems. If a feature request is for a domain about to be migrated, wait for the extraction and build it in the new service.

---

### Category G: Advanced Scenarios

**Q20: How would you apply the Strangler Fig pattern to a frontend (not just backend APIs)?**

A: Micro-frontends with module federation:
1. Deploy a shell application that loads frontend modules dynamically.
2. Migrate one page/module at a time from the legacy SPA to a new framework.
3. The shell routes to legacy or new modules based on URL path.
4. Shared state managed via events or a shared store.
Alternatively, for simpler cases: use an NGINX reverse proxy to serve `/checkout` from the new app and everything else from the legacy app. The user sees a seamless experience.

**Q21: How do you handle the Strangler Fig pattern with SOAP/XML legacy services?**

A: The facade acts as a protocol translator:
1. External clients call the facade via REST/JSON.
2. For non-migrated domains, the facade translates to SOAP/XML and calls the legacy service.
3. For migrated domains, the facade routes directly to the new REST service.
4. Spring Cloud Gateway with custom filters or Apache Camel can handle SOAP-to-REST transformation.
The ACL can also live in a sidecar that translates between protocols.

**Q22: How would you apply the Strangler Fig pattern if the monolith has no API and all communication is via shared database tables?**

A: This is the hardest scenario. Steps:
1. **Create an API layer first** (Branch by Abstraction): Introduce an internal API in the monolith that wraps database access for the target domain.
2. **Other modules call the API** instead of direct SQL. This creates the seam.
3. **Extract the API into a microservice** behind a facade (standard Strangler Fig from here).
4. **Replace direct DB access with CDC events** for downstream consumers.
Without an API layer, there is no request to intercept, and the facade pattern cannot work.

**Q23: Netflix took 8 years to complete their migration. Is that normal?**

A: For Netflix's scale (700+ services, global infrastructure), yes. But for most companies:
- Single bounded context: 1-3 months
- Medium domain: 3-6 months
- Full monolith decomposition: 12-36 months
The timeline depends on: monolith complexity, team size, data coupling, organizational alignment, and whether you're migrating all domains or just the most painful ones.
Key lesson from Netflix: they built new features as microservices first. The monolith naturally shrank as new functionality bypassed it. See [Section 16.1](#161-netflix-2008-2016).

**Q24: You are a lead joining a team midway through a Strangler Fig migration. The migration has stalled at 30%. What do you do in your first month?**

A: Assessment and unblocking:
1. **Week 1**: Map the current state -- which domains are migrated, which are in progress, which are blocked. Talk to every team.
2. **Week 2**: Identify the top 3 blockers. Common ones: shared database coupling, lack of CDC, unclear domain boundaries, team capacity.
3. **Week 3**: Address the blockers. Set up CDC if missing. Define clear bounded contexts for the next 2-3 extractions. Create the migration runbook if it doesn't exist.
4. **Week 4**: Present a revised roadmap with quarterly milestones, blockers addressed, and resource requirements. Get executive sponsorship for the decommission date.
5. **Ongoing**: Weekly migration metrics dashboard (% traffic on new, reconciliation status, deployment frequency).

**Q25: How do you ensure zero downtime during the final cutover from legacy to the new service?**

A: The facade makes this trivial:
1. Traffic is already at 100% on the new service (ramp completed).
2. Legacy is running but receiving 0% traffic (hot standby).
3. Wait for the stabilization period (2-4 weeks at 100%).
4. Remove the legacy route from the facade configuration. Deploy.
5. The legacy service is now orphaned. No traffic reaches it.
6. Stop the legacy service process. No user impact.
7. Drop the legacy database tables (after backup).
There is no "cutover moment" with downtime because the traffic shift already happened gradually.

---

### Category H: Curveball Questions

**Q26: Can you apply the Strangler Fig pattern in reverse -- migrating FROM microservices TO a monolith?**

A: Yes. If microservices were premature and the overhead exceeds the benefits, you can use the same pattern in reverse:
1. Build the monolith module alongside the microservices.
2. Route traffic from individual microservices to the monolith.
3. Decommission the microservices as functionality moves into the monolith.
This is sometimes called the "macro-service" or "modular monolith" trend. The facade-based routing works identically in both directions.

**Q27: How does the Strangler Fig pattern relate to Domain-Driven Design?**

A: DDD provides the **analytical framework** for the Strangler Fig pattern:
1. **Bounded Contexts** define what to extract -- each extraction should align with a bounded context.
2. **Ubiquitous Language** guides the new service's domain model -- don't copy legacy naming.
3. **Context Maps** identify relationships between bounded contexts and where ACLs are needed.
4. **Event Storming** is the best workshop format to identify domain boundaries before starting the migration.
Without DDD, you risk extracting arbitrary code slices instead of coherent domain units.

**Q28: What's the difference between the Strangler Fig pattern and the Parallel Run pattern?**

A: They solve different problems and are often used together:
- **Strangler Fig**: Incrementally replaces a system by routing traffic from old to new.
- **Parallel Run**: Both systems process the same request simultaneously; outputs are compared for verification.
In practice, Parallel Run is a **verification technique within** the Strangler Fig's Coexist phase. You run both systems in parallel (shadow mode), compare outputs, and only increase traffic to the new system once outputs match. See [Section 9](#section-9-testing-and-verification).

**Q29: Your architect says "let's just add a new database and dual-write to both." What do you say?**

A: Respectfully push back. Dual writes are fundamentally unsafe:
1. No atomicity across two databases without distributed transactions (2PC).
2. If write to DB1 succeeds but DB2 fails, the stores diverge silently.
3. Under load, write ordering can diverge.
4. Retry logic for failed writes is complex and error-prone.
Instead, use CDC: write to one database (the source of truth), and stream changes to the other via Debezium/Kafka. The second database is a derived replica. This is ordered, consistent, and non-invasive. See [Section 7.6](#76-why-not-dual-writes).

**Q30: What metrics would you track to prove the Strangler Fig migration is succeeding?**

A: Four categories:
1. **Migration progress**: % traffic on new services by domain (track weekly, target 100%).
2. **Quality parity**: Error rate comparison (new <= legacy), latency comparison (new p95 <= legacy p95 * 1.1), shadow match rate (>= 99%).
3. **Engineering velocity**: Deployment frequency (should increase), lead time for changes (should decrease), incident count for migrated domains (should decrease).
4. **Business impact**: Conversion rate, revenue, customer satisfaction scores -- unchanged or improved after each migration step.

---

## Appendix: Quick Reference Cheat Sheet

### Strangler Fig Decision Tree

```
Is the legacy system causing pain?
├── No → Don't migrate. Maintain as-is.
└── Yes
    ├── Is it small/simple? → Direct rewrite may be okay.
    └── Is it large/complex/business-critical?
        ├── Can you identify bounded contexts? → Strangler Fig
        └── No clear boundaries?
            ├── Do Event Storming workshop → Identify domains → Strangler Fig
            └── Monolith is too tangled → Branch by Abstraction first → Then Strangler Fig
```

### Migration Speed Checklist

| Factor | Fast | Slow |
|--------|------|------|
| Domain boundaries | Clear bounded contexts | Tangled spaghetti |
| Database | Separate tables per domain | Shared tables with cross-domain FKs |
| API layer | REST/gRPC APIs exist | Direct DB access everywhere |
| Test coverage | Good automated tests | Manual testing only |
| Team structure | Domain-aligned teams | Functional silos |
| Executive support | Committed sponsor with timeline | "When you get around to it" |

### Key Terminology

| Term | Definition |
|------|-----------|
| Facade | Routing proxy in front of both systems |
| ACL | Anti-Corruption Layer -- adapter between legacy and new domain models |
| CDC | Change Data Capture -- streaming DB changes via WAL/binlog |
| Shadow traffic | Sending copies of requests to new system without returning its response |
| Parallel run | Both systems process same request; outputs compared |
| Canary | Small % of traffic to new system for validation |
| Parity | Behavioral equivalence between legacy and new system |
| Decommission | Removing legacy code, routes, and data after successful migration |
| Seam | A point in the monolith where you can insert a boundary for extraction |
