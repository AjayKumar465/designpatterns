package com.designpatterns.demo.consumer;

import com.designpatterns.demo.greeting.GreetingService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class DemoConsumerApplicationTest {

    @Autowired
    private GreetingService greetingService;

    @Test
    void starterCreatesGreetingService() {
        assertThat(greetingService.greet("Ajay")).isEqualTo("Hello, Ajay!");
    }
}
