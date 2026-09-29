package com.example.harness;

import com.example.harness.config.HarnessConfig;
import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Cucumber-Spring glue: the harness's own Spring context, built from the
 * explicitly wired {@link HarnessConfig} (not from app autoconfiguration).
 */
@CucumberContextConfiguration
@SpringBootTest(classes = HarnessConfig.class)
public class CucumberSpringConfig {
}
