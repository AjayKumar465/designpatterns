# Demo Custom Spring Boot Starter

This example shows how to build a production-style custom Spring Boot starter.

It contains:

1. `demo-greeting-library`: a normal Java library with `GreetingService`.
2. `demo-greeting-spring-boot-autoconfigure`: auto-configures `GreetingService`.
3. `demo-greeting-spring-boot-starter`: dependency bundle used by applications.
4. `demo-consumer-app`: Spring Boot app that depends only on the starter.

## Why This Demo Matters

The consumer app does not directly depend on `demo-greeting-library` or `demo-greeting-spring-boot-autoconfigure`.

It declares only:

```xml
<dependency>
  <groupId>com.designpatterns.demo</groupId>
  <artifactId>demo-greeting-spring-boot-starter</artifactId>
  <version>${project.version}</version>
</dependency>
```

Maven pulls the library and autoconfigure module transitively. Spring Boot discovers the auto-configuration from:

```text
META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

## Run

From this directory:

```bash
mvn clean verify
mvn -pl demo-consumer-app -am spring-boot:run
```

Then open:

```text
http://localhost:8080/greet/Ajay
```

Expected response:

```text
Hello, Ajay!
```

## Try Configuration Override

Run:

```bash
mvn -pl demo-consumer-app -am spring-boot:run -Dspring-boot.run.arguments="--demo.greeting.prefix=Namaste"
```

Expected response:

```text
Namaste, Ajay!
```

## Disable Auto-Configuration

Run:

```bash
mvn -pl demo-consumer-app -am spring-boot:run -Dspring-boot.run.arguments="--demo.greeting.enabled=false"
```

The app will fail because the consumer controller requires `GreetingService`, but the starter intentionally does not create it when disabled.

That is a good test: the feature flag is actually respected.
