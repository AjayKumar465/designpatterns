package com.designpatterns.demo.greeting.autoconfigure;

import com.designpatterns.demo.greeting.DefaultGreetingService;
import com.designpatterns.demo.greeting.GreetingService;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

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
