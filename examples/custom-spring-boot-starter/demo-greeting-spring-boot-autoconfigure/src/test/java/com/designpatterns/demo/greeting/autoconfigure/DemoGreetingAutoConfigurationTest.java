package com.designpatterns.demo.greeting.autoconfigure;

import com.designpatterns.demo.greeting.GreetingService;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class DemoGreetingAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DemoGreetingAutoConfiguration.class));

    @Test
    void createsGreetingServiceByDefault() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(GreetingService.class);
            assertThat(context.getBean(GreetingService.class).greet("Ajay"))
                .isEqualTo("Hello, Ajay!");
        });
    }

    @Test
    void usesConfiguredPrefix() {
        contextRunner
            .withPropertyValues("demo.greeting.prefix=Namaste")
            .run(context -> assertThat(context.getBean(GreetingService.class).greet("Ajay"))
                .isEqualTo("Namaste, Ajay!"));
    }

    @Test
    void backsOffWhenUserProvidesGreetingService() {
        contextRunner
            .withBean(GreetingService.class, () -> name -> "Custom " + name)
            .run(context -> assertThat(context.getBean(GreetingService.class).greet("Ajay"))
                .isEqualTo("Custom Ajay"));
    }

    @Test
    void doesNotCreateGreetingServiceWhenDisabled() {
        contextRunner
            .withPropertyValues("demo.greeting.enabled=false")
            .run(context -> assertThat(context).doesNotHaveBean(GreetingService.class));
    }
}
