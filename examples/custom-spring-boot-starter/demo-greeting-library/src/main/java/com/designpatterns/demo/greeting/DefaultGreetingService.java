package com.designpatterns.demo.greeting;

import java.util.Objects;

public final class DefaultGreetingService implements GreetingService {

    private final String prefix;

    public DefaultGreetingService(String prefix) {
        this.prefix = Objects.requireNonNull(prefix, "prefix must not be null");
    }

    @Override
    public String greet(String name) {
        String normalizedName = name == null || name.isBlank() ? "friend" : name.trim();
        return prefix + ", " + normalizedName + "!";
    }
}
