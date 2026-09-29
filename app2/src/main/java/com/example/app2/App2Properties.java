package com.example.app2;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized pipeline parameters for App2.
 *
 * <p>The defaults mirror application.yml; they keep the app bootable
 * in-JVM under the harness even if this module's application.yml is
 * shadowed on the shared test classpath by app1's application.yml.</p>
 */
@ConfigurationProperties(prefix = "app2")
public class App2Properties {

    private String inputTopic = "b";
    private String outputTopic = "c";
    private int constant = 100;

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

    public int getConstant() {
        return constant;
    }

    public void setConstant(int constant) {
        this.constant = constant;
    }
}
