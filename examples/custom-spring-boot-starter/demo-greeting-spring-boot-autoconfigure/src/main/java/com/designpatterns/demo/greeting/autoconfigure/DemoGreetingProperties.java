package com.designpatterns.demo.greeting.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "demo.greeting")
public class DemoGreetingProperties {

    /**
     * Whether the demo greeting auto-configuration should create a GreetingService.
     */
    private boolean enabled = true;

    /**
     * Prefix used by the default GreetingService.
     */
    private String prefix = "Hello";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
