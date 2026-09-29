package com.example.app1;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized pipeline parameters for App1.
 *
 * <p>The defaults mirror application.yml; they keep the app bootable
 * in-JVM under the harness even if this module's application.yml is
 * shadowed on the shared test classpath by app2's application.yml.</p>
 */
@ConfigurationProperties(prefix = "app1")
public class App1Properties {

    private String inputTopic = "a";
    private String outputTopic = "b";
    private int multiplier = 2;

    public String getInputTopic() {
        return inputTopic;
    }

    public void setInputTopic(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public int getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(int multiplier) {
        this.multiplier = multiplier;
    }
}
