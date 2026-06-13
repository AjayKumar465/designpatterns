# Custom Spring Boot Starter — Expert Playbook (Lead/Architect, Java, 10+ Years)

Build production-grade Spring Boot starters that teams add with one dependency and get auto-configured beans, sensible defaults, and override points. Based on the runnable demo in `examples/custom-spring-boot-starter/`.

---

## Table of Contents

1. [What Is a Spring Boot Starter?](#1-what-is-a-spring-boot-starter)
2. [Starter vs Dependency vs Autoconfigure](#2-starter-vs-dependency-vs-autoconfigure)
3. [When to Build a Custom Starter](#3-when-to-build-a-custom-starter)
4. [Module Layout and Naming Rules](#4-module-layout-and-naming-rules)
5. [Demo Architecture Walkthrough](#5-demo-architecture-walkthrough)
6. [Step 1 — Library Module (Business API)](#6-step-1--library-module-business-api)
7. [Step 2 — Properties and Metadata](#7-step-2--properties-and-metadata)
8. [Step 3 — Auto-Configuration Class](#8-step-3--auto-configuration-class)
9. [Step 4 — Registration (AutoConfiguration.imports)](#9-step-4--registration-autoconfigurationimports)
10. [Step 5 — Starter POM (Dependency Bundle)](#10-step-5--starter-pom-dependency-bundle)
11. [Step 6 — Consumer Application](#11-step-6--consumer-application)
12. [Conditional Annotations Deep Dive](#12-conditional-annotations-deep-dive)
13. [Testing Starters with ApplicationContextRunner](#13-testing-starters-with-applicationcontextrunner)
14. [Optional BOM for Dependency Management](#14-optional-bom-for-dependency-management)
15. [Publishing and Versioning](#15-publishing-and-versioning)
16. [Production Patterns — Health, Metrics, Feature Flags](#16-production-patterns--health-metrics-feature-flags)
17. [Production Pitfalls](#17-production-pitfalls)
18. [Lead Interview Questions & Answers](#18-lead-interview-questions--answers)
19. [How to Talk About Custom Starters in an Interview](#19-how-to-talk-about-custom-starters-in-an-interview)

---

## 1. What Is a Spring Boot Starter?

A **starter** is a Maven/Gradle dependency that bundles libraries and triggers **auto-configuration** so consumers get working integration with minimal setup.

Consumer adds one line:

```xml
<dependency>
  <groupId>com.designpatterns.demo</groupId>
  <artifactId>demo-greeting-spring-boot-starter</artifactId>
</dependency>
```

They get:

1. Transitive library dependencies
2. Auto-configured beans (if conditions match)
3. Configurable properties with defaults
4. Ability to override beans

---

## 2. Starter vs Dependency vs Autoconfigure

| Artifact | Role | Contains logic? |
|---|---|---|
| `*-library` | Business API / client | Yes — interfaces, implementations |
| `*-autoconfigure` | Spring configuration | Yes — `@AutoConfiguration`, conditions |
| `*-starter` | Dependency aggregator | No — only POM dependencies |
| Regular dependency | Single library | Yes — one concern |

**Analogy:** Starter = combo meal. Autoconfigure = recipe. Library = ingredients.

---

## 3. When to Build a Custom Starter

Build a starter when:

- Same 50–100 lines of config copied across 5+ services
- Platform team owns observability, security, or messaging setup
- You publish internal SDKs to many teams
- Version alignment matters (BOM + starter)

Do **not** build a starter when:

- Only one app uses the integration
- Configuration is trivial (one `@Bean` in the app)

---

## 4. Module Layout and Naming Rules

```text
demo-greeting-spring-boot/
  pom.xml                          (parent)
  demo-greeting-library/           (pure Java API)
  demo-greeting-spring-boot-autoconfigure/
  demo-greeting-spring-boot-starter/
  demo-greeting-spring-boot-dependencies/  (optional BOM)
  demo-consumer-app/               (example consumer)
```

**Naming rules:**

| Rule | Example |
|---|---|
| Never prefix with `spring-boot-*` for third-party | ❌ `spring-boot-acme-starter` |
| Use company prefix | ✅ `acme-observability-spring-boot-starter` |
| Autoconfigure suffix | `*-spring-boot-autoconfigure` |
| Starter suffix | `*-spring-boot-starter` |

---

## 5. Demo Architecture Walkthrough

```
demo-consumer-app
       │
       │ depends on
       ▼
demo-greeting-spring-boot-starter  (POM only — pulls transitives)
       │
       ├── demo-greeting-spring-boot-autoconfigure
       │         └── DemoGreetingAutoConfiguration
       └── demo-greeting-library
                 └── GreetingService / DefaultGreetingService
```

Spring Boot reads:

```text
META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

Run demo:

```bash
cd examples/custom-spring-boot-starter
mvn clean verify
mvn -pl demo-consumer-app -am spring-boot:run
curl http://localhost:8080/greet/Ajay
# Hello, Ajay!
```

---

## 6. Step 1 — Library Module (Business API)

Keep the library **free of Spring annotations** when possible — easier to test and reuse.

```java
// demo-greeting-library
public interface GreetingService {
    String greet(String name);
}

public class DefaultGreetingService implements GreetingService {
    private final String prefix;

    public DefaultGreetingService(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String greet(String name) {
        return prefix + ", " + name + "!";
    }
}
```

---

## 7. Step 2 — Properties and Metadata

```java
@ConfigurationProperties(prefix = "demo.greeting")
public class DemoGreetingProperties {
    private boolean enabled = true;
    private String prefix = "Hello";

    // getters/setters
}
```

`META-INF/additional-spring-configuration-metadata.json` for IDE hints:

```json
{
  "properties": [
    {
      "name": "demo.greeting.enabled",
      "type": "java.lang.Boolean",
      "defaultValue": true,
      "description": "Whether greeting auto-configuration is active."
    },
    {
      "name": "demo.greeting.prefix",
      "type": "java.lang.String",
      "defaultValue": "Hello",
      "description": "Prefix used in greeting messages."
    }
  ]
}
```

---

## 8. Step 3 — Auto-Configuration Class

From repo `DemoGreetingAutoConfiguration.java`:

```java
@AutoConfiguration
@ConditionalOnClass(GreetingService.class)
@ConditionalOnProperty(
    prefix = "demo.greeting",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true
)
@EnableConfigurationProperties(DemoGreetingProperties.class)
public class DemoGreetingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    GreetingService greetingService(DemoGreetingProperties properties) {
        return new DefaultGreetingService(properties.getPrefix());
    }
}
```

**Key points:**

- `@AutoConfiguration` (Spring Boot 3+) replaces `@Configuration` for auto-config classes
- `@ConditionalOnMissingBean` — user override wins
- `@ConditionalOnProperty` — feature flag support

---

## 9. Step 4 — Registration (AutoConfiguration.imports)

```text
# META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
com.designpatterns.demo.greeting.autoconfigure.DemoGreetingAutoConfiguration
```

Spring Boot 3+ uses this file instead of `spring.factories`.

---

## 10. Step 5 — Starter POM (Dependency Bundle)

```xml
<artifactId>demo-greeting-spring-boot-starter</artifactId>

<dependencies>
  <dependency>
    <groupId>com.designpatterns.demo</groupId>
    <artifactId>demo-greeting-spring-boot-autoconfigure</artifactId>
  </dependency>
  <dependency>
    <groupId>com.designpatterns.demo</groupId>
    <artifactId>demo-greeting-library</artifactId>
  </dependency>
</dependencies>
```

Starter has **no Java source** — only dependency declarations.

---

## 11. Step 6 — Consumer Application

```java
@RestController
class GreetingController {
    private final GreetingService greetingService;

    GreetingController(GreetingService greetingService) {
        this.greetingService = greetingService;
    }

    @GetMapping("/greet/{name}")
    String greet(@PathVariable String name) {
        return greetingService.greet(name);
    }
}
```

Override prefix:

```bash
mvn -pl demo-consumer-app -am spring-boot:run \
  -Dspring-boot.run.arguments="--demo.greeting.prefix=Namaste"
```

Disable auto-config:

```yaml
demo:
  greeting:
    enabled: false
```

---

## 12. Conditional Annotations Deep Dive

| Annotation | Meaning |
|---|---|
| `@ConditionalOnClass` | Classpath has class |
| `@ConditionalOnMissingClass` | Classpath lacks class |
| `@ConditionalOnBean` | Bean already exists |
| `@ConditionalOnMissingBean` | No bean of type — create default |
| `@ConditionalOnProperty` | Property matches |
| `@ConditionalOnWebApplication` | Running as web app |
| `@ConditionalOnResource` | File exists on classpath |

**Order matters:** Use `@AutoConfigureBefore` / `@AutoConfigureAfter` when your config must run relative to Spring Boot's own auto-config.

---

## 13. Testing Starters with ApplicationContextRunner

From `DemoGreetingAutoConfigurationTest.java` pattern:

```java
class DemoGreetingAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DemoGreetingAutoConfiguration.class));

    @Test
    void createsGreetingServiceByDefault() {
        contextRunner.run(ctx -> assertThat(ctx).hasSingleBean(GreetingService.class));
    }

    @Test
    void respectsEnabledFlag() {
        contextRunner
            .withPropertyValues("demo.greeting.enabled=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(GreetingService.class));
    }

    @Test
    void userBeanOverridesDefault() {
        contextRunner
            .withBean(GreetingService.class, () -> name -> "Custom, " + name)
            .run(ctx -> assertThat(ctx.getBean(GreetingService.class).greet("X"))
                .isEqualTo("Custom, X"));
    }
}
```

**Do not** start full `@SpringBootTest` for every starter test — too slow.

---

## 14. Optional BOM for Dependency Management

When starter pulls versions outside Spring Boot BOM:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.acme.platform</groupId>
      <artifactId>acme-observability-spring-boot-dependencies</artifactId>
      <version>1.0.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

---

## 15. Publishing and Versioning

```bash
mvn clean verify
mvn install        # local
mvn deploy         # CI → Nexus/Artifactory
```

**Versioning:** Semantic versioning. Breaking property renames = major bump.

---

## 16. Production Patterns — Health, Metrics, Feature Flags

### Health indicator (optional)

```java
@Bean
@ConditionalOnClass(HealthIndicator.class)
HealthIndicator acmeClientHealth(AcmeClient client) {
    return () -> client.ping()
        ? Health.up().build()
        : Health.down().withDetail("reason", "unreachable").build();
}
```

### Micrometer metrics

```java
@Bean
MeterBinder acmeClientMetrics(AcmeClient client) {
    return registry -> client.bindMetrics(registry);
}
```

### Feature flags

Always expose `enabled` property defaulting to `true` with `matchIfMissing = true` only if safe.

---

## 17. Production Pitfalls

| Pitfall | Fix |
|---|---|
| Auto-config not loading | Check `AutoConfiguration.imports` path and class name |
| Bean not overridable | Add `@ConditionalOnMissingBean` |
| Starter pulls too much | Keep starter thin; optional deps as `optional=true` |
| Properties not in IDE | Add `spring-configuration-metadata.json` |
| Tests start full app | Use `ApplicationContextRunner` |
| Circular dependency | Split config classes; use `@Lazy` sparingly |
| Breaking change in patch | Follow semver; document property renames |

---

## 18. Lead Interview Questions & Answers

**Q1: What is the difference between a starter and autoconfigure module?**

**A**: Starter is a POM that pulls dependencies. Autoconfigure contains `@AutoConfiguration` classes, conditions, and property binding. Consumer only declares starter. *(Sections 2, 10)*

**Q2: How does Spring Boot discover your auto-configuration?**

**A**: `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` lists fully qualified class names. On startup, Spring Boot loads and evaluates conditions. *(Section 9)*

**Q3: How can a consumer override your default bean?**

**A**: Define their own `@Bean` of the same type. Your config uses `@ConditionalOnMissingBean` so theirs wins. *(Section 8)*

**Q4: How do you test a starter without `@SpringBootTest`?**

**A**: `ApplicationContextRunner` with `AutoConfigurations.of(YourAutoConfiguration.class)`. Test default bean, disabled flag, and user override. *(Section 13)*

**Q5: Why not put `@Configuration` in the starter JAR directly?**

**A**: Separation lets library work without Spring, autoconfigure be tested in isolation, and starter stay a thin dependency bundle. *(Section 4)*

**Q6: `@ConditionalOnProperty` vs `@Profile`?**

**A**: Property flags are explicit and documented in metadata. Profiles are environment-based (dev/prod). Use properties for feature toggles; profiles for environment-specific beans. *(Section 12)*

**Q7: How do you avoid pulling unwanted transitive dependencies?**

**A**: Mark optional dependencies, split into multiple starters (`-starter` vs `-starter-web`), use `optional=true` in POM. *(Section 10)*

**Q8: Spring Boot 2 vs 3 auto-config registration?**

**A**: Boot 2 used `META-INF/spring.factories`. Boot 3 uses `AutoConfiguration.imports`. Migrate during Boot 3 upgrade. *(Section 9)*

**Q9: What is `@AutoConfigureBefore` / `@AutoConfigureAfter` for?**

**A**: Control ordering when your beans depend on another auto-config running first (e.g., after `DataSourceAutoConfiguration`). Prevents `@DependsOn` hacks. *(Section 12)*

**Q10: How do you disable your starter in one app?**

**A**: `demo.greeting.enabled=false` via `@ConditionalOnProperty`, or exclude via `@SpringBootApplication(exclude = DemoGreetingAutoConfiguration.class)`. *(Sections 8, 11)*

**Q11: Should the library module depend on Spring?**

**A**: No when possible. Keep business API Spring-free. Only autoconfigure module depends on `spring-boot-autoconfigure`. *(Sections 4, 6)*

**Q12: How do you add optional dependencies?**

**A**: Mark dependency `optional=true` in autoconfigure POM. Wrap beans with `@ConditionalOnClass`. Consumer adds explicit dependency if they want that feature. *(Section 10)*

**Q13: What goes in `spring-configuration-metadata.json`?**

**A**: Property names, types, defaults, descriptions for IDE autocomplete. Does not affect runtime — documentation only. *(Section 7)*

**Q14: How do you version breaking property renames?**

**A**: Semver major bump. Support deprecated alias for one release with `@DeprecatedConfigurationProperty` if Boot supports it, document migration. *(Section 15)*

**Q15: Can consumers use `@ConfigurationProperties` on your properties class?**

**A**: Your auto-config uses `@EnableConfigurationProperties`. Consumer can inject `DemoGreetingProperties` if exposed as a bean, or bind their own copy — prefer exposing properties bean for advanced users. *(Section 7)*

**Q16: How do you avoid creating beans in tests that don't want them?**

**A**: `@ImportAutoConfiguration` selectively, or `@MockBean GreetingService`, or exclude auto-config in `@SpringBootTest`. Unit tests use `ApplicationContextRunner`. *(Section 13)*

**Q17: Difference between `@Configuration` and `@AutoConfiguration`?**

**A**: `@AutoConfiguration` is ordered, loaded from imports file, intended for Boot's auto-config pipeline. Regular `@Configuration` in app code loads differently. *(Section 8)*

**Q18: How do you publish to Maven Central / internal Nexus?**

**A**: `mvn deploy` with distributionManagement in parent POM. CI signs artifacts. Document coordinates in README. *(Section 15)*

**Q19: What if two starters both create the same bean type?**

**A**: `@ConditionalOnMissingBean` — first wins or user defines override. Document bean names. Use `@Primary` only as last resort. *(Section 8)*

**Q20: How do you integrate Actuator health with a starter client?**

**A**: Optional `HealthIndicator` bean guarded by `@ConditionalOnClass(HealthIndicator.class)`. Ping remote service in `health()`. *(Section 16)*

**Q21: GraalVM native image considerations?**

**A**: Register reflection hints in `META-INF/native-image/` if library uses reflection. Test native build in CI for platform starters. *(Section 17)*

**Q22: How does Spring Boot 3 `@AutoConfiguration` differ from `@Configuration` in starter?**

**A**: Auto-config classes are processed in a specific order with exclusions. They should not be component-scanned — only listed in imports file. *(Section 8)*

**Q23: Multi-module Maven reactor build order?**

**A**: Parent POM lists modules: library → autoconfigure → starter → consumer. `mvn -pl demo-consumer-app -am` builds dependencies. *(Section 5)*

**Q24: When would you split into multiple starters?**

**A**: When optional integrations bloat classpath — e.g., `acme-starter-core` vs `acme-starter-web` vs `acme-starter-reactive`. *(Section 10)*

**Q25: How do you debug "bean not created" in consumer app?**

**A**: Enable `debug=true`, check conditions report, verify imports file on classpath, check `@ConditionalOnProperty` values, verify starter dependency not excluded. *(Section 17)*

---

## 19. How to Talk About Custom Starters in an Interview

> Plain English. Short sentences.

---

### "What is a custom Spring Boot starter?"

It's a dependency your team publishes. Other apps add it to their pom.xml and get automatic setup — beans, config, libraries — without copying the same code everywhere.

---

### "How does auto-configuration work?"

Spring Boot looks for a registration file inside your JAR. It lists your config class. Spring runs that class but only creates beans if conditions pass — like "this property is enabled" or "user hasn't already defined this bean".

---

### "Starter vs regular dependency?"

A starter bundles multiple libraries and turns on auto-config. A regular dependency is just one library. `spring-boot-starter-actuator` is a starter. `micrometer-registry-prometheus` is a regular dependency.

---

### Quick Answers

| Question | Say this |
|---|---|
| What is a starter? | One dependency that auto-configures a feature with sensible defaults |
| Two modules? | autoconfigure = logic, starter = dependency bundle |
| How discovered? | AutoConfiguration.imports file in META-INF/spring |
| User override? | @ConditionalOnMissingBean — their bean wins |
| How to test? | ApplicationContextRunner — fast, no full app boot |
| Naming rule? | company-feature-spring-boot-starter — not spring-boot-* |

---

*Runnable demo: `examples/custom-spring-boot-starter/` — run `mvn clean verify` then `mvn -pl demo-consumer-app -am spring-boot:run`*
