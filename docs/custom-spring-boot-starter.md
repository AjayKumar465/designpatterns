# Custom Spring Boot Starter: Production Playbook

## Goal

Build your own Spring Boot starter so an application can add one dependency and get:

1. Required libraries pulled transitively.
2. Version alignment through dependency management.
3. Auto-configuration activated only when the right classes/properties are present.
4. Sensible defaults that users can override.
5. Production-grade tests, metadata, publishing, and upgrade discipline.

Example starter in this guide:

1. `acme-observability-spring-boot-autoconfigure`
2. `acme-observability-spring-boot-starter`
3. Optional `acme-observability-spring-boot-dependencies` BOM

Important naming rule:

1. Do not name third-party starters `spring-boot-*`.
2. Use your company/product prefix, for example `acme-*-spring-boot-starter`.

---

## Mental Model

Spring Boot starters usually have two roles:

1. `*-autoconfigure`: contains actual auto-configuration classes, properties, conditions, and tests.
2. `*-starter`: contains no business logic; it is a dependency bundle that pulls `*-autoconfigure` plus runtime libraries.

For dependency management:

1. If all dependencies are managed by Spring Boot's BOM, the app usually does not need your BOM.
2. If your starter depends on libraries outside Spring Boot's BOM, publish your own BOM.
3. The consuming app imports your BOM or uses a company parent POM so versions remain centralized.

When the app adds only this dependency:

```xml
<dependency>
  <groupId>com.acme.platform</groupId>
  <artifactId>acme-observability-spring-boot-starter</artifactId>
  <version>1.0.0</version>
</dependency>
```

Maven/Gradle resolves transitive dependencies declared by the starter. Spring Boot then discovers your auto-configuration from:

```text
META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

---

## Module Layout

```text
acme-observability-spring-boot/
  pom.xml
  acme-observability-spring-boot-dependencies/
    pom.xml
  acme-observability-spring-boot-autoconfigure/
    pom.xml
    src/main/java/com/acme/observability/autoconfigure/
      AcmeObservabilityAutoConfiguration.java
      AcmeObservabilityProperties.java
      AcmeTracingClient.java
      NoopAcmeTracingClient.java
    src/main/resources/
      META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
      META-INF/spring-configuration-metadata.json
    src/test/java/com/acme/observability/autoconfigure/
      AcmeObservabilityAutoConfigurationTest.java
  acme-observability-spring-boot-starter/
    pom.xml
```

Staff-level rule:

1. Keep starter module thin.
2. Keep auto-configuration testable without starting a full application.

---

## Step 1: Parent Build

Use the Spring Boot parent if this repository only builds the starter family.

```xml
<!-- acme-observability-spring-boot/pom.xml -->
<project>
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.acme.platform</groupId>
  <artifactId>acme-observability-spring-boot</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.0</version>
    <relativePath/>
  </parent>

  <modules>
    <module>acme-observability-spring-boot-dependencies</module>
    <module>acme-observability-spring-boot-autoconfigure</module>
    <module>acme-observability-spring-boot-starter</module>
  </modules>

  <properties>
    <java.version>17</java.version>
    <acme.tracing.version>2.4.1</acme.tracing.version>
  </properties>
</project>
```

Production notes:

1. Spring Boot 3.x requires Java 17 or newer.
2. Keep Spring Boot version explicit and upgrade it deliberately.
3. Avoid using snapshot versions in released starters.

---

## Step 2: Optional BOM for Dependency Management

Create a BOM when your starter owns versions for third-party or internal libraries.

```xml
<!-- acme-observability-spring-boot-dependencies/pom.xml -->
<project>
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.acme.platform</groupId>
    <artifactId>acme-observability-spring-boot</artifactId>
    <version>1.0.0</version>
  </parent>

  <artifactId>acme-observability-spring-boot-dependencies</artifactId>
  <packaging>pom</packaging>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.acme.platform</groupId>
        <artifactId>acme-tracing-client</artifactId>
        <version>${acme.tracing.version}</version>
      </dependency>
    </dependencies>
  </dependencyManagement>
</project>
```

Consumer usage with Maven:

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

What this does:

1. The BOM manages versions.
2. The starter pulls dependencies.
3. The application keeps its own dependency list small.

Common mistake:

1. A starter dependency pulls libraries transitively, but it does not globally manage versions for dependencies the app declares separately.
2. Use a BOM when you need centralized version alignment beyond the starter's direct dependency tree.

---

## Step 3: Autoconfigure Module POM

```xml
<!-- acme-observability-spring-boot-autoconfigure/pom.xml -->
<project>
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.acme.platform</groupId>
    <artifactId>acme-observability-spring-boot</artifactId>
    <version>1.0.0</version>
  </parent>

  <artifactId>acme-observability-spring-boot-autoconfigure</artifactId>

  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-autoconfigure</artifactId>
    </dependency>

    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-configuration-processor</artifactId>
      <optional>true</optional>
    </dependency>

    <dependency>
      <groupId>com.acme.platform</groupId>
      <artifactId>acme-tracing-client</artifactId>
      <optional>true</optional>
    </dependency>

    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
</project>
```

Why `optional=true` here:

1. The autoconfigure module should compile against optional integrations.
2. The starter module decides which libraries are actually pulled into applications.
3. Conditions such as `@ConditionalOnClass` prevent configuration from activating when optional libraries are absent.

---

## Step 4: Configuration Properties

```java
package com.acme.observability.autoconfigure;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "acme.observability")
public class AcmeObservabilityProperties {

    /**
     * Enables Acme observability auto-configuration.
     */
    private boolean enabled = true;

    /**
     * Collector endpoint used by the tracing client.
     */
    private String endpoint = "http://localhost:4318";

    /**
     * Timeout for outbound telemetry calls.
     */
    private Duration timeout = Duration.ofSeconds(2);

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }
}
```

Production rules:

1. Use a unique prefix owned by your team.
2. Defaults must be safe.
3. Properties should be documented for IDE metadata.
4. Do not read environment variables manually; let Spring Boot bind configuration.

---

## Step 5: Auto-Configuration Class

```java
package com.acme.observability.autoconfigure;

import com.acme.tracing.AcmeTracingClient;
import com.acme.tracing.DefaultAcmeTracingClient;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnClass(AcmeTracingClient.class)
@ConditionalOnProperty(
    prefix = "acme.observability",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true
)
@EnableConfigurationProperties(AcmeObservabilityProperties.class)
public class AcmeObservabilityAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    AcmeTracingClient acmeTracingClient(AcmeObservabilityProperties properties) {
        return new DefaultAcmeTracingClient(
            properties.getEndpoint(),
            properties.getTimeout()
        );
    }
}
```

Condition rules:

1. Use `@ConditionalOnClass` before touching optional library types.
2. Use `@ConditionalOnMissingBean` so application teams can override your bean.
3. Use `@ConditionalOnProperty` for feature flags.
4. Avoid broad component scanning from starters.

At Google scale, the most painful starter bugs are usually not syntax issues. They are accidental bean creation, unexpected classpath activation, and no escape hatch for service teams.

---

## Step 6: Register Auto-Configuration

Create:

```text
src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

Content:

```text
com.acme.observability.autoconfigure.AcmeObservabilityAutoConfiguration
```

Modern Spring Boot uses `AutoConfiguration.imports` for auto-configuration discovery.

Legacy note:

1. Spring Boot 2.x commonly used `META-INF/spring.factories`.
2. For modern Boot 3.x and 4.x starters, use `AutoConfiguration.imports`.
3. Only add legacy registration if you intentionally support older Boot 2.x applications.

---

## Step 7: Add Configuration Metadata

The configuration processor can generate metadata during build. You can also add hints manually.

```json
{
  "properties": [
    {
      "name": "acme.observability.enabled",
      "type": "java.lang.Boolean",
      "description": "Whether Acme observability auto-configuration is enabled.",
      "defaultValue": true
    },
    {
      "name": "acme.observability.endpoint",
      "type": "java.lang.String",
      "description": "Collector endpoint used by the tracing client.",
      "defaultValue": "http://localhost:4318"
    },
    {
      "name": "acme.observability.timeout",
      "type": "java.time.Duration",
      "description": "Timeout for outbound telemetry calls.",
      "defaultValue": "2s"
    }
  ]
}
```

Why this matters:

1. IDE autocomplete works.
2. Platform teams reduce support tickets.
3. Configuration drift becomes easier to spot.

---

## Step 8: Starter Module POM

The starter pulls everything the consumer should get automatically.

```xml
<!-- acme-observability-spring-boot-starter/pom.xml -->
<project>
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.acme.platform</groupId>
    <artifactId>acme-observability-spring-boot</artifactId>
    <version>1.0.0</version>
  </parent>

  <artifactId>acme-observability-spring-boot-starter</artifactId>

  <dependencies>
    <dependency>
      <groupId>com.acme.platform</groupId>
      <artifactId>acme-observability-spring-boot-autoconfigure</artifactId>
      <version>${project.version}</version>
    </dependency>

    <dependency>
      <groupId>com.acme.platform</groupId>
      <artifactId>acme-tracing-client</artifactId>
    </dependency>

    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
  </dependencies>
</project>
```

This is the "automatic dependency pull" mechanism:

1. Application declares the starter.
2. Maven/Gradle resolves the starter's transitive dependencies.
3. `acme-tracing-client`, actuator, and autoconfigure jar arrive on the app classpath.
4. Spring Boot discovers auto-configuration from the autoconfigure jar.
5. Beans are created only if conditions match.

---

## Step 9: Consumer Application Usage

Maven:

```xml
<dependencies>
  <dependency>
    <groupId>com.acme.platform</groupId>
    <artifactId>acme-observability-spring-boot-starter</artifactId>
    <version>1.0.0</version>
  </dependency>
</dependencies>
```

Gradle:

```groovy
dependencies {
    implementation "com.acme.platform:acme-observability-spring-boot-starter:1.0.0"
}
```

Application configuration:

```yaml
acme:
  observability:
    enabled: true
    endpoint: https://otel-collector.internal:4318
    timeout: 2s
```

Override example:

```java
@Bean
AcmeTracingClient acmeTracingClient() {
    return new CustomAcmeTracingClient();
}
```

Because the starter uses `@ConditionalOnMissingBean`, this custom bean wins.

---

## Step 10: Test Auto-Configuration

Use `ApplicationContextRunner` instead of full integration tests for most starter behavior.

```java
package com.acme.observability.autoconfigure;

import com.acme.tracing.AcmeTracingClient;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class AcmeObservabilityAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(AcmeObservabilityAutoConfiguration.class));

    @Test
    void createsTracingClientByDefault() {
        contextRunner.run(context -> assertThat(context)
            .hasSingleBean(AcmeTracingClient.class));
    }

    @Test
    void backsOffWhenUserProvidesTracingClient() {
        contextRunner
            .withBean(AcmeTracingClient.class, TestAcmeTracingClient::new)
            .run(context -> assertThat(context)
                .hasSingleBean(AcmeTracingClient.class)
                .getBean(AcmeTracingClient.class)
                .isInstanceOf(TestAcmeTracingClient.class));
    }

    @Test
    void doesNotCreateBeanWhenDisabled() {
        contextRunner
            .withPropertyValues("acme.observability.enabled=false")
            .run(context -> assertThat(context)
                .doesNotHaveBean(AcmeTracingClient.class));
    }
}
```

Minimum test matrix:

1. Default bean is created.
2. Feature flag disables configuration.
3. User-provided bean wins.
4. Required class missing means auto-configuration backs off.
5. Property binding validates expected values.
6. Starter dependency tree contains expected transitive dependencies.

---

## Step 11: Add Health and Metrics When Useful

If the starter owns a network client, add optional production signals.

Example:

```java
@Bean
@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
@ConditionalOnMissingBean(name = "acmeObservabilityHealthIndicator")
HealthIndicator acmeObservabilityHealthIndicator(AcmeTracingClient client) {
    return () -> client.isReady()
        ? Health.up().build()
        : Health.down().withDetail("reason", "collector unavailable").build();
}
```

Rules:

1. Health checks must be cheap.
2. Do not block startup on remote systems unless the user opts in.
3. Add Micrometer metrics for request count, latency, failures, and queue depth if applicable.

---

## Step 12: Publish Artifacts

Local verification:

```bash
mvn clean verify
mvn install
```

Publish to internal Maven repository:

```bash
mvn deploy
```

Repository configuration usually lives in:

```xml
<distributionManagement>
  <repository>
    <id>internal-releases</id>
    <url>https://repo.acme.internal/releases</url>
  </repository>
  <snapshotRepository>
    <id>internal-snapshots</id>
    <url>https://repo.acme.internal/snapshots</url>
  </snapshotRepository>
</distributionManagement>
```

Consumer repository configuration:

```xml
<repositories>
  <repository>
    <id>internal-releases</id>
    <url>https://repo.acme.internal/releases</url>
  </repository>
</repositories>
```

Production release rules:

1. Sign artifacts if publishing publicly.
2. Generate source and javadoc jars for public libraries.
3. Tag every release in Git.
4. Never overwrite released versions.
5. Use semantic versioning and publish release notes.

---

## Step 13: Verify Dependency Pull-Through

After adding the starter to a consumer app, verify Maven dependency tree:

```bash
mvn dependency:tree -Dincludes=com.acme.platform
```

Expected shape:

```text
com.myapp:order-service:jar:1.0.0
\- com.acme.platform:acme-observability-spring-boot-starter:jar:1.0.0
   +- com.acme.platform:acme-observability-spring-boot-autoconfigure:jar:1.0.0
   +- com.acme.platform:acme-tracing-client:jar:2.4.1
   \- org.springframework.boot:spring-boot-starter-actuator:jar:...
```

Verify auto-configuration at runtime:

```bash
java -jar app.jar --debug
```

Or inspect the Actuator conditions endpoint:

```text
GET /actuator/conditions
```

Look for:

```text
AcmeObservabilityAutoConfiguration matched
```

---

## Step 14: Production Hardening Checklist

Required:

1. `@ConditionalOnClass` around optional libraries.
2. `@ConditionalOnMissingBean` for user override.
3. `@ConditionalOnProperty` for feature flag.
4. `@ConfigurationProperties` with metadata.
5. `ApplicationContextRunner` tests.
6. Dependency tree verification.
7. Explicit compatibility matrix.
8. Release notes and migration guide.

Recommended:

1. Actuator health indicator for external clients.
2. Micrometer metrics.
3. Failure analyzer for common misconfiguration.
4. BOM if you manage non-Boot dependency versions.
5. Sample consumer application in the repo.

---

## Compatibility Matrix

Example:

| Starter Version | Spring Boot | Java | Notes |
| --- | --- | --- | --- |
| `1.0.x` | `3.3.x` to `3.5.x` | `17+` | Initial production line |
| `2.0.x` | `4.0.x` | `17+` or project standard | Validate package and dependency changes |

Rules:

1. Test against the lowest and highest supported Spring Boot versions.
2. Do not promise compatibility you do not run in CI.
3. Publish a new major version for breaking property names or bean behavior.

---

## Common Production Bugs

1. Starter creates beans even when the user already has one.
2. Auto-configuration references a missing class directly and crashes during class loading.
3. Starter pulls a conflicting library version because there is no BOM discipline.
4. Defaults connect to a production endpoint from local developer machines.
5. Configuration properties are undocumented, so users guess and cargo-cult values.
6. Starter uses component scanning and accidentally registers internal classes.
7. Tests use `@SpringBootTest` only and miss condition-level behavior.

---

## Interview-Grade Explanation

If asked "How does a custom Spring Boot starter work?", answer like this:

1. The starter is a dependency bundle. It pulls autoconfigure and runtime libraries transitively.
2. The autoconfigure jar contributes classes listed in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
3. Spring Boot imports those classes during startup.
4. Conditions decide whether each configuration applies.
5. `@ConfigurationProperties` binds external config into typed objects.
6. `@ConditionalOnMissingBean` gives application code final control.
7. A BOM or parent POM manages versions; the starter itself pulls dependencies.

Push beyond textbook answer:

1. The hardest part is not creating a bean.
2. The hard part is creating it only when safe, making it easy to override, testing all condition paths, and avoiding dependency conflicts across hundreds of services.

## Official References

1. Spring Boot auto-configuration: https://docs.spring.io/spring-boot/reference/using/auto-configuration.html
2. Creating your own auto-configuration: https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html
3. Spring Boot build systems and starters: https://docs.spring.io/spring-boot/4.0/reference/using/build-systems.html
4. Spring Boot Gradle dependency management: https://docs.spring.io/spring-boot/gradle-plugin/managing-dependencies.html

---

## How to Talk About Custom Spring Boot Starters in an Interview (Human English)

---

### "What is a Spring Boot Starter and why would you build one?"

> "Spring Boot starters are those `spring-boot-starter-*` dependencies you add to your project. When you add `spring-boot-starter-data-jpa`, it pulls in JPA, Hibernate, transaction management, and auto-configures a DataSource for you — you just set the URL in `application.yml`. A custom starter is you building that same experience for your company's internal libraries. Instead of every team copying the same 100 lines of configuration code to set up, say, your company's audit logging or observability setup, you package it once as a starter and they just add one dependency and it works. Convention over configuration, internal edition."

---

### "How does auto-configuration actually work?"

> "Spring Boot's auto-configuration is powered by `@ConditionalOn*` annotations. When the starter's JAR is on the classpath, Spring finds the auto-configuration class via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. But the beans don't all blindly activate — they're conditional. `@ConditionalOnMissingBean(DataSource.class)` means 'only create this DataSource if the user hasn't defined one already'. `@ConditionalOnProperty('acme.feature.enabled', havingValue='true')` means 'only activate this if the user opts in via a property'. This way the starter provides sensible defaults but the user can override anything. That's the contract."

---

### "What's the naming convention and why does it matter?"

> "Third-party starters (yours and mine) should be named `acme-spring-boot-starter`. Spring's own starters are `spring-boot-starter-*`. Never start your starter name with `spring-boot` — that namespace is reserved for the Spring team. The auto-configure module should be separate from the starter module: `acme-observability-spring-boot-autoconfigure` holds the `@Configuration` classes and conditions. `acme-observability-spring-boot-starter` is just a thin POM that depends on the autoconfigure module plus required libraries. Users add the starter; the autoconfigure activates transparently."

---

### Quick Cheat Sheet

| Question | One-line answer |
|---|---|
| What is a starter? | A dependency that auto-configures library integration with sensible defaults |
| How does auto-config work? | `@ConditionalOn*` annotations + `AutoConfiguration.imports` registration |
| Naming rule? | `acme-spring-boot-starter` — never prefix with `spring-boot-*` |
| User override? | `@ConditionalOnMissingBean` lets user's own bean take precedence |
| Two module pattern? | `-autoconfigure` (logic) + `-starter` (dependency aggregator) |
| When to build one? | When the same config is copy-pasted across multiple internal services |

